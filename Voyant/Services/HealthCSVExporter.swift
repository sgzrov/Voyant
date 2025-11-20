import Foundation
import HealthKit

struct HealthCSVExporter {

    // MARK: - Public API
    // Generates CSV matching the required schema by querying ~20 HealthKit metrics.
    // - Last 14 days: hourly
    // - Previous 150 days: every 4 hours + daily aggregate
    static func generateCSV(for userId: String, metrics requestedMetrics: [String], completion: @escaping (Result<Data, Error>) -> Void) {
        let healthStore = HKHealthStore()
        let tz = TimeZone.current.identifier
        let createdAt = ISO8601DateFormatter().string(from: Date())
        let iso = ISO8601DateFormatter()

        let now = Date()
        guard let start164 = Calendar.current.date(byAdding: .day, value: -164, to: now),
              let start14 = Calendar.current.date(byAdding: .day, value: -14, to: now) else {
            completion(.failure(NSError(domain: "csv", code: -1, userInfo: [NSLocalizedDescriptionKey: "Date math failed"])));
            return
        }

        var rows: [String] = ["user_id,timestamp,metric_type,metric_value,unit,source,timezone,created_at"]

        let specs = MetricSpec.defaultSpecs().filter { requestedMetrics.isEmpty ? true : requestedMetrics.contains($0.name) }

        let group = DispatchGroup()
        let encounteredError: Error? = nil

        for spec in specs {
            // Choose intervals per metric:
            // - heart_rate, oxygen_saturation: 1m (last 14), 30m (prev 150)
            // - additive metrics (sum): 5m (last 14), 60m (prev 150)
            // - other continuous: 5m (last 14), 30m (prev 150)
            let isHighFreq = (spec.name == "heart_rate" || spec.name == "oxygen_saturation")
            let isAdditive = (spec.aggregation == .sum)
            let interval14: DateComponents = isHighFreq
                ? DateComponents(minute: 1)
                : (isAdditive ? DateComponents(minute: 5) : DateComponents(minute: 5))
            let interval150: DateComponents = isHighFreq
                ? DateComponents(minute: 30)
                : (isAdditive ? DateComponents(hour: 1) : DateComponents(minute: 30))
            // Hourly for last 14 days
            group.enter()
            queryQuantityOrCategory(
                healthStore: healthStore,
                spec: spec,
                start: start14,
                end: now,
                interval: interval14,
                aggregation: spec.aggregation
            ) { result in
                switch result {
                case .success(let points):
                    points.forEach { p in
                        let line = "\(userId),\(iso.string(from: p.timestamp)),\(spec.name),\(formatValue(p.value)),\(spec.unitLabel),\(p.source),\(tz),\(createdAt)"
                        rows.append(line)
                    }
                case .failure(let error):
                    // Non-fatal: ignore per-metric authorization or data errors
                    print("[HealthCSVExporter] hourly last14 '\(spec.name)' error: \(error.localizedDescription)")
                }
                group.leave()
            }

            // Every 4 hours for previous 150 days
            group.enter()
            queryQuantityOrCategory(
                healthStore: healthStore,
                spec: spec,
                start: start164,
                end: start14,
                interval: interval150,
                aggregation: spec.aggregation
            ) { result in
                switch result {
                case .success(let points):
                    points.forEach { p in
                        let line = "\(userId),\(iso.string(from: p.timestamp)),\(spec.name),\(formatValue(p.value)),\(spec.unitLabel),\(p.source),\(tz),\(createdAt)"
                        rows.append(line)
                    }
                case .failure(let error):
                    print("[HealthCSVExporter] four-hour prev150 '\(spec.name)' error: \(error.localizedDescription)")
                }
                group.leave()
            }
        }

        // MARK: - Workouts + simple events
        group.enter()
        let workoutType = HKObjectType.workoutType()
        let workoutPredicate = HKQuery.predicateForSamples(withStart: start164, end: now, options: .strictStartDate)
        let sort = NSSortDescriptor(key: HKSampleSortIdentifierStartDate, ascending: true)
        let workoutQuery = HKSampleQuery(sampleType: workoutType, predicate: workoutPredicate, limit: HKObjectQueryNoLimit, sortDescriptors: [sort]) { _, samples, error in
            defer { group.leave() }
            if let error = error {
                // Non-fatal: skip workouts if unauthorized
                print("[HealthCSVExporter] workouts query error: \(error.localizedDescription)")
                return
            }
            guard let workouts = samples as? [HKWorkout] else { return }
            for w in workouts {
                let ts = iso.string(from: w.startDate)
                let typeLabel = Self.workoutActivityName(w.workoutActivityType)
                let distKm = (w.totalDistance?.doubleValue(for: HKUnit.meter()) ?? 0.0) / 1000.0
                let durMin = w.duration / 60.0
                var kcal: Double = 0.0
                if let qt = HKQuantityType.quantityType(forIdentifier: .activeEnergyBurned) {
                    kcal = w.statistics(for: qt)?.sumQuantity()?.doubleValue(for: .kilocalorie()) ?? 0.0
                }

                rows.append("\(userId),\(ts),workout_distance_km,\(formatValue(distKm)),km,\(typeLabel),\(tz),\(createdAt)")
                rows.append("\(userId),\(ts),workout_duration_min,\(formatValue(durMin)),min,\(typeLabel),\(tz),\(createdAt)")
                rows.append("\(userId),\(ts),workout_energy_kcal,\(formatValue(kcal)),kcal,\(typeLabel),\(tz),\(createdAt)")

                // Simple events
                if distKm >= 10.0 {
                    rows.append("\(userId),\(ts),event_long_run_km,\(formatValue(distKm)),km,\(typeLabel),\(tz),\(createdAt)")
                }
                if kcal >= 800.0 || durMin >= 60.0 {
                    rows.append("\(userId),\(ts),event_hard_workout,1,count,\(typeLabel),\(tz),\(createdAt)")
                }
            }
        }
        healthStore.execute(workoutQuery)

        group.notify(queue: .main) {
            if let error = encounteredError {
                completion(.failure(error))
                return
            }
            let data = rows.joined(separator: "\n").data(using: .utf8) ?? Data()
            completion(.success(data))
        }
    }

    // MARK: - Delta Exporter (hourly only within [start, end])
    static func generateDeltaCSV(for userId: String,
                                 start: Date,
                                 end: Date,
                                 metrics requestedMetrics: [String] = [],
                                 minuteResolution: Bool = false,
                                 completion: @escaping (Result<Data, Error>) -> Void) {
        let healthStore = HKHealthStore()
        let tz = TimeZone.current.identifier
        let createdAt = ISO8601DateFormatter().string(from: Date())
        let iso = ISO8601DateFormatter()

        var rows: [String] = ["user_id,timestamp,metric_type,metric_value,unit,source,timezone,created_at"]

        let specs = MetricSpec.defaultSpecs().filter { requestedMetrics.isEmpty ? true : requestedMetrics.contains($0.name) }

        let group = DispatchGroup()
        let encounteredError: Error? = nil

        for spec in specs {
            group.enter()
            queryQuantityOrCategory(
                healthStore: healthStore,
                spec: spec,
                start: start,
                end: end,
                interval: minuteResolution ? DateComponents(minute: 1) : DateComponents(hour: 1),
                aggregation: spec.aggregation
            ) { result in
                switch result {
                case .success(let points):
                    points.forEach { p in
                        let line = "\(userId),\(iso.string(from: p.timestamp)),\(spec.name),\(formatValue(p.value)),\(spec.unitLabel),\(p.source),\(tz),\(createdAt)"
                        rows.append(line)
                    }
                case .failure(let error):
                    // Non-fatal: ignore per-metric authorization or data errors
                    print("[HealthCSVExporter] delta hourly '\(spec.name)' error: \(error.localizedDescription)")
                }
                group.leave()
            }
        }

        // Workouts within window
        group.enter()
        let workoutType = HKObjectType.workoutType()
        let workoutPredicate = HKQuery.predicateForSamples(withStart: start, end: end, options: .strictStartDate)
        let sort = NSSortDescriptor(key: HKSampleSortIdentifierStartDate, ascending: true)
        let workoutQuery = HKSampleQuery(sampleType: workoutType, predicate: workoutPredicate, limit: HKObjectQueryNoLimit, sortDescriptors: [sort]) { _, samples, error in
            defer { group.leave() }
            if let error = error {
                print("[HealthCSVExporter] delta workouts query error: \(error.localizedDescription)")
                return
            }
            guard let workouts = samples as? [HKWorkout] else { return }
            for w in workouts {
                let ts = iso.string(from: w.startDate)
                let typeLabel = Self.workoutActivityName(w.workoutActivityType)
                let distKm = (w.totalDistance?.doubleValue(for: HKUnit.meter()) ?? 0.0) / 1000.0
                let durMin = w.duration / 60.0
                var kcal: Double = 0.0
                if let qt = HKQuantityType.quantityType(forIdentifier: .activeEnergyBurned) {
                    kcal = w.statistics(for: qt)?.sumQuantity()?.doubleValue(for: .kilocalorie()) ?? 0.0
                }
                rows.append("\(userId),\(ts),workout_distance_km,\(formatValue(distKm)),km,\(typeLabel),\(tz),\(createdAt)")
                rows.append("\(userId),\(ts),workout_duration_min,\(formatValue(durMin)),min,\(typeLabel),\(tz),\(createdAt)")
                rows.append("\(userId),\(ts),workout_energy_kcal,\(formatValue(kcal)),kcal,\(typeLabel),\(tz),\(createdAt)")
                if distKm >= 10.0 {
                    rows.append("\(userId),\(ts),event_long_run_km,\(formatValue(distKm)),km,\(typeLabel),\(tz),\(createdAt)")
                }
                if kcal >= 800.0 || durMin >= 60.0 {
                    rows.append("\(userId),\(ts),event_hard_workout,1,count,\(typeLabel),\(tz),\(createdAt)")
                }
            }
        }
        healthStore.execute(workoutQuery)

        group.notify(queue: .main) {
            if let error = encounteredError {
                completion(.failure(error))
                return
            }
            let data = rows.joined(separator: "\n").data(using: .utf8) ?? Data()
            completion(.success(data))
        }
    }


    private struct MetricSpec {
        enum Aggregation { case average, sum }
        let name: String
        let quantityType: HKQuantityType?
        let categoryType: HKCategoryType?
        let unit: HKUnit?
        let unitLabel: String
        let aggregation: Aggregation
        let allowZero: Bool

        static func defaultSpecs() -> [MetricSpec] {
            var list: [MetricSpec] = []
            func q(_ id: HKQuantityTypeIdentifier) -> HKQuantityType { HKObjectType.quantityType(forIdentifier: id)! }
            func c(_ id: HKCategoryTypeIdentifier) -> HKCategoryType { HKObjectType.categoryType(forIdentifier: id)! }

            list.append(MetricSpec(name: "heart_rate",               quantityType: q(.heartRate),               categoryType: nil, unit: HKUnit.count().unitDivided(by: .minute()), unitLabel: "bpm",   aggregation: .average, allowZero: false))
            list.append(MetricSpec(name: "resting_heart_rate",       quantityType: q(.restingHeartRate),        categoryType: nil, unit: HKUnit.count().unitDivided(by: .minute()), unitLabel: "bpm",   aggregation: .average, allowZero: false))
            list.append(MetricSpec(name: "walking_hr_avg",           quantityType: q(.walkingHeartRateAverage), categoryType: nil, unit: HKUnit.count().unitDivided(by: .minute()), unitLabel: "bpm",   aggregation: .average, allowZero: false))
            list.append(MetricSpec(name: "hr_variability_sdnn",      quantityType: q(.heartRateVariabilitySDNN),categoryType: nil, unit: HKUnit.secondUnit(with: .milli),          unitLabel: "ms",    aggregation: .average, allowZero: false))
            list.append(MetricSpec(name: "steps",                    quantityType: q(.stepCount),              categoryType: nil, unit: HKUnit.count(),                          unitLabel: "count", aggregation: .sum,    allowZero: true))
            list.append(MetricSpec(name: "walking_speed",            quantityType: q(.walkingSpeed),           categoryType: nil, unit: HKUnit.meter().unitDivided(by: .second()), unitLabel: "m_per_s",aggregation: .average, allowZero: false))
            list.append(MetricSpec(name: "vo2_max",                  quantityType: q(.vo2Max),                 categoryType: nil, unit: HKUnit(from: "ml/(kg*min)"),             unitLabel: "ml_per_kg_min", aggregation: .average, allowZero: false))
            list.append(MetricSpec(name: "active_energy_burned",     quantityType: q(.activeEnergyBurned),     categoryType: nil, unit: HKUnit.kilocalorie(),                     unitLabel: "kcal",  aggregation: .sum,    allowZero: true))
            // Standalone distances to support inferred sessions when no HKWorkout exists
            list.append(MetricSpec(name: "distance_walking_running_km", quantityType: q(.distanceWalkingRunning), categoryType: nil, unit: HKUnit.meterUnit(with: .kilo), unitLabel: "km", aggregation: .sum, allowZero: true))
            if let qc = HKObjectType.quantityType(forIdentifier: .distanceCycling) {
                list.append(MetricSpec(name: "distance_cycling_km", quantityType: qc, categoryType: nil, unit: HKUnit.meterUnit(with: .kilo), unitLabel: "km", aggregation: .sum, allowZero: true))
            }
            if let qs = HKObjectType.quantityType(forIdentifier: .distanceSwimming) {
                list.append(MetricSpec(name: "distance_swimming_km", quantityType: qs, categoryType: nil, unit: HKUnit.meterUnit(with: .kilo), unitLabel: "km", aggregation: .sum, allowZero: true))
            }
            list.append(MetricSpec(name: "dietary_water",            quantityType: q(.dietaryWater),           categoryType: nil, unit: HKUnit.liter(),                           unitLabel: "L",     aggregation: .sum,    allowZero: true))
            list.append(MetricSpec(name: "body_mass",                quantityType: q(.bodyMass),               categoryType: nil, unit: HKUnit.gramUnit(with: .kilo),             unitLabel: "kg",    aggregation: .average, allowZero: false))
            list.append(MetricSpec(name: "body_mass_index",          quantityType: q(.bodyMassIndex),          categoryType: nil, unit: HKUnit.count(),                          unitLabel: "bmi",   aggregation: .average, allowZero: false))
            list.append(MetricSpec(name: "blood_glucose",            quantityType: q(.bloodGlucose),           categoryType: nil, unit: HKUnit(from: "mg/dL"),                    unitLabel: "mg_dL", aggregation: .average, allowZero: false))
            list.append(MetricSpec(name: "oxygen_saturation",        quantityType: q(.oxygenSaturation),       categoryType: nil, unit: HKUnit.percent(),                          unitLabel: "percent",aggregation: .average, allowZero: false))
            list.append(MetricSpec(name: "blood_pressure_systolic",  quantityType: q(.bloodPressureSystolic),  categoryType: nil, unit: HKUnit.millimeterOfMercury(),             unitLabel: "mmHg",  aggregation: .average, allowZero: false))
            list.append(MetricSpec(name: "blood_pressure_diastolic", quantityType: q(.bloodPressureDiastolic), categoryType: nil, unit: HKUnit.millimeterOfMercury(),             unitLabel: "mmHg",  aggregation: .average, allowZero: false))
            list.append(MetricSpec(name: "respiratory_rate",         quantityType: q(.respiratoryRate),        categoryType: nil, unit: HKUnit.count().unitDivided(by: .minute()), unitLabel: "breaths_per_min", aggregation: .average, allowZero: false))
            list.append(MetricSpec(name: "body_temperature",         quantityType: q(.bodyTemperature),        categoryType: nil, unit: HKUnit.degreeCelsius(),                   unitLabel: "degC",  aggregation: .average, allowZero: false))
            list.append(MetricSpec(name: "mindfulness_minutes",      quantityType: nil,                        categoryType: c(.mindfulSession), unit: HKUnit.minute(),            unitLabel: "min",   aggregation: .sum,    allowZero: false))
            list.append(MetricSpec(name: "sleep_hours",              quantityType: nil,                        categoryType: c(.sleepAnalysis),  unit: HKUnit.hour(),              unitLabel: "hours", aggregation: .sum,    allowZero: false))
            list.append(MetricSpec(name: "active_time_minutes",      quantityType: q(.appleExerciseTime),      categoryType: nil, unit: HKUnit.minute(),                           unitLabel: "min",   aggregation: .sum,    allowZero: true))
            return list
        }
    }

    // MARK: - Query Engine
    private struct DataPoint { let timestamp: Date; let value: Double; let source: String }

    private static func queryQuantityOrCategory(healthStore: HKHealthStore,
                                                spec: MetricSpec,
                                                start: Date,
                                                end: Date,
                                                interval: DateComponents,
                                                aggregation: MetricSpec.Aggregation,
                                                completion: @escaping (Result<[DataPoint], Error>) -> Void) {
        if let qt = spec.quantityType {
            queryQuantity(healthStore: healthStore, quantityType: qt, unit: spec.unit, start: start, end: end, interval: interval, aggregation: aggregation, allowZero: spec.allowZero, completion: completion)
        } else if let ct = spec.categoryType {
            if ct.identifier == HKCategoryTypeIdentifier.sleepAnalysis.rawValue {
                querySleep(healthStore: healthStore, start: start, end: end, interval: interval, completion: completion)
            } else if ct.identifier == HKCategoryTypeIdentifier.mindfulSession.rawValue {
                queryCategoryDuration(healthStore: healthStore, categoryType: ct, unit: spec.unit, start: start, end: end, interval: interval, completion: completion)
            } else {
                completion(.success([]))
            }
        } else {
            completion(.success([]))
        }
    }

    private static func queryQuantity(healthStore: HKHealthStore,
                                      quantityType: HKQuantityType,
                                      unit: HKUnit?,
                                      start: Date,
                                      end: Date,
                                      interval: DateComponents,
                                      aggregation: MetricSpec.Aggregation,
                                      allowZero: Bool,
                                      completion: @escaping (Result<[DataPoint], Error>) -> Void) {
        let predicate = HKQuery.predicateForSamples(withStart: start, end: end, options: .strictStartDate)
        let anchorDate = start
        let statsOptions: HKStatisticsOptions = (aggregation == .sum) ? .cumulativeSum : .discreteAverage

        let query = HKStatisticsCollectionQuery(quantityType: quantityType, quantitySamplePredicate: predicate, options: statsOptions, anchorDate: anchorDate, intervalComponents: interval)
        query.initialResultsHandler = { _, results, error in
            if let error = error { completion(.failure(error)); return }
            var out: [DataPoint] = []
            results?.enumerateStatistics(from: start, to: end) { stats, _ in
                let ts = stats.startDate
                var maybeValue: Double?
                if aggregation == .sum, let sumQ = stats.sumQuantity() {
                    let v = sumQ.doubleValue(for: unit ?? HKUnit.count())
                    if v > 0 || (v == 0 && allowZero) {
                        maybeValue = v
                    }
                }
                if aggregation == .average, let avgQ = stats.averageQuantity() {
                    let v = avgQ.doubleValue(for: unit ?? HKUnit.count())
                    maybeValue = v
                }
                if var value = maybeValue {
                    if quantityType.identifier == HKQuantityTypeIdentifier.oxygenSaturation.rawValue {
                        // Convert fraction to percent
                        value *= 100.0
                    }
                    out.append(DataPoint(timestamp: ts, value: value, source: "Apple Watch"))
                }
            }
            completion(.success(out))
        }
        healthStore.execute(query)
    }

    private static func querySleep(healthStore: HKHealthStore,
                                   start: Date,
                                   end: Date,
                                   interval: DateComponents,
                                   completion: @escaping (Result<[DataPoint], Error>) -> Void) {
        // Group sleep category samples into buckets and sum duration (hours).
        let type = HKObjectType.categoryType(forIdentifier: .sleepAnalysis)!
        let predicate = HKQuery.predicateForSamples(withStart: start, end: end, options: .strictStartDate)
        let sort = NSSortDescriptor(key: HKSampleSortIdentifierStartDate, ascending: true)
        let query = HKSampleQuery(sampleType: type, predicate: predicate, limit: HKObjectQueryNoLimit, sortDescriptors: [sort]) { _, samples, error in
            if let error = error { completion(.failure(error)); return }

            let cal = Calendar.current
            var bucketStart = start
            var out: [DataPoint] = []
            while bucketStart < end {
                let bucketEnd = cal.date(byAdding: interval, to: bucketStart) ?? end
                let window = samples?.compactMap { $0 as? HKCategorySample }.filter { s in
                    return s.startDate < bucketEnd && s.endDate > bucketStart
                } ?? []
                var seconds: Double = 0
                for s in window {
                    let overlapStart = max(bucketStart, s.startDate)
                    let overlapEnd = min(bucketEnd, s.endDate)
                    seconds += max(0, overlapEnd.timeIntervalSince(overlapStart))
                }
                let hours = seconds / 3600.0
                if hours > 0 {
                    out.append(DataPoint(timestamp: bucketStart, value: hours, source: "Apple Watch"))
                }
                bucketStart = bucketEnd
            }
            completion(.success(out))
        }
        healthStore.execute(query)
    }

    private static func queryCategoryDuration(healthStore: HKHealthStore,
                                              categoryType: HKCategoryType,
                                              unit: HKUnit?,
                                              start: Date,
                                              end: Date,
                                              interval: DateComponents,
                                              completion: @escaping (Result<[DataPoint], Error>) -> Void) {
        // Sum duration of category samples within bucket (e.g., mindful minutes)
        let predicate = HKQuery.predicateForSamples(withStart: start, end: end, options: .strictStartDate)
        let sort = NSSortDescriptor(key: HKSampleSortIdentifierStartDate, ascending: true)
        let query = HKSampleQuery(sampleType: categoryType, predicate: predicate, limit: HKObjectQueryNoLimit, sortDescriptors: [sort]) { _, samples, error in
            if let error = error { completion(.failure(error)); return }
            let cal = Calendar.current
            var bucketStart = start
            var out: [DataPoint] = []
            while bucketStart < end {
                let bucketEnd = cal.date(byAdding: interval, to: bucketStart) ?? end
                let window = samples?.compactMap { $0 as? HKCategorySample }.filter { s in
                    return s.startDate < bucketEnd && s.endDate > bucketStart
                } ?? []
                var seconds: Double = 0
                for s in window {
                    let overlapStart = max(bucketStart, s.startDate)
                    let overlapEnd = min(bucketEnd, s.endDate)
                    seconds += max(0, overlapEnd.timeIntervalSince(overlapStart))
                }
                let minutes = seconds / 60.0
                if minutes > 0 {
                    out.append(DataPoint(timestamp: bucketStart, value: minutes, source: "Apple Watch"))
                }
                bucketStart = bucketEnd
            }
            completion(.success(out))
        }
        healthStore.execute(query)
    }

    private static func formatValue(_ value: Double) -> String {
        if value.isFinite == false { return "" }
        return String(format: "%.4f", value)
    }

    private static func workoutActivityName(_ type: HKWorkoutActivityType) -> String {
        switch type {
        case .running: return "running"
        case .walking: return "walking"
        case .cycling: return "cycling"
        case .swimming: return "swimming"
        case .hiking: return "hiking"
        case .elliptical: return "elliptical"
        case .yoga: return "yoga"
        case .traditionalStrengthTraining: return "strength"
        case .downhillSkiing: return "skiing_downhill"
        case .crossCountrySkiing: return "skiing_xc"
        case .snowboarding: return "snowboarding"
        default: return "workout"
        }
    }
}
