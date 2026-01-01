import Foundation
import HealthKit
import CoreLocation
#if canImport(UIKit)
import UIKit
#endif

struct HealthCSVExporter {

    private static func csvField(_ s: String?) -> String {
        // Keep it simple: we currently don't emit commas in tz identifiers; if we ever do, we'd need quoting.
        return (s ?? "").replacingOccurrences(of: "\n", with: " ").replacingOccurrences(of: "\r", with: " ")
    }

    /// HealthKit unit debug strings can contain conversion metadata like `mmol<180.15588>/L`.
    /// For storage/display we want a clean, stable label like `mmol/L`.
    private static func cleanedHKUnitLabel(_ unit: HKUnit) -> String {
        var s = String(describing: unit)

        // Strip any `<...>` segments (may include molar mass etc.)
        while let lt = s.firstIndex(of: "<"), let gt = s[lt...].firstIndex(of: ">") {
            s.removeSubrange(lt...gt)
        }

        // Common cosmetic normalizations
        if s == "count/min" { return "bpm" }
        return s
    }

    private static func workoutTimezoneFromGeoHistory(for date: Date) -> (tzName: String?, utcOffsetMin: Int?) {
        // Prefer geo-derived timezone history (most accurate). If unavailable/denied, fall back to device tz history,
        // but ONLY when it is actually known for that timestamp (never guess for older history).
        if let name = GeoTimezoneHistoryService.shared.tzNameIfKnown(for: date),
           let tz = TimeZone(identifier: name) {
            return (tz.identifier, tz.secondsFromGMT(for: date) / 60)
        }
        if let name = TimezoneHistoryService.shared.tzNameIfKnown(for: date),
           let tz = TimeZone(identifier: name) {
            return (tz.identifier, tz.secondsFromGMT(for: date) / 60)
        }
        return (nil, nil)
    }

    private static func workoutTimezoneFromRoute(healthStore: HKHealthStore, workout: HKWorkout, completion: @escaping (String?, Int?) -> Void) {
        // Defensive: never hang exporter if HealthKit route/geocoding never calls back.
        let lock = NSLock()
        var finished = false
        let timeout = DispatchWorkItem {
            var shouldCall = false
            lock.lock()
            if !finished {
                finished = true
                shouldCall = true
            }
            lock.unlock()
            if shouldCall { completion(nil, nil) }
        }
        DispatchQueue.global(qos: .utility).asyncAfter(deadline: .now() + 3.0, execute: timeout)

        func finish(_ tzName: String?, _ utcOffsetMin: Int?) {
            var shouldCall = false
            lock.lock()
            if !finished {
                finished = true
                shouldCall = true
            }
            lock.unlock()
            if shouldCall {
                timeout.cancel()
                completion(tzName, utcOffsetMin)
            }
        }

        let routeType = HKSeriesType.workoutRoute()
        let predicate = HKQuery.predicateForObjects(from: workout)
        let routeQuery = HKSampleQuery(sampleType: routeType, predicate: predicate, limit: 1, sortDescriptors: nil) { _, samples, _ in
            guard let route = (samples as? [HKWorkoutRoute])?.first else {
                finish(nil, nil)
                return
            }

            var firstLocation: CLLocation?
            let locQuery = HKWorkoutRouteQuery(route: route) { _, locationsOrNil, done, _ in
                if firstLocation == nil, let loc = locationsOrNil?.first {
                    firstLocation = loc
                }
                if done {
                    guard let loc = firstLocation else {
                        finish(nil, nil)
                        return
                    }
                    #if canImport(UIKit)
                    CLGeocoder().reverseGeocodeLocation(loc) { placemarks, _ in
                        let tz = placemarks?.first?.timeZone
                        if let tz = tz {
                            finish(tz.identifier, tz.secondsFromGMT(for: workout.startDate) / 60)
                        } else {
                            finish(nil, nil)
                        }
                    }
                    #else
                    _ = loc
                    finish(nil, nil)
                    #endif
                }
            }
            healthStore.execute(locQuery)
        }
        healthStore.execute(routeQuery)
    }

    // MARK: - Public API
    // Generates CSV for a metrics-only backend:
    // - Last 60 days hourly: steps (SUM), active_energy_burned (SUM), heart_rate (AVG), oxygen_saturation (AVG optional), distances, active_time_minutes (SUM)
    // - Last 60 days daily: sleep_hours (SUM, attach to bucket start), resting_heart_rate (AVG), hr_variability_sdnn (AVG)
    static func generateCSV(for userId: String, metrics requestedMetrics: [String], completion: @escaping (Result<Data, Error>) -> Void) {
        let healthStore = HKHealthStore()
        let createdAt = ISO8601DateFormatter().string(from: Date())
        let iso = ISO8601DateFormatter()
        print("[HealthCSVExporter] generateCSV start user=\(userId) requestedMetrics=\(requestedMetrics.count)")
        // Decide cadence per metric: minute for fast-changing, daily for daily metrics, hourly otherwise
        let minutelyNames: Set<String> = [
            "heart_rate",
            "steps",
            "active_energy_burned",
            "walking_speed",
            "distance_walking_running_km",
            "distance_cycling_km",
            "distance_swimming_km",
            "active_time_minutes"
        ]
        let dailyNames: Set<String> = [
            "sleep_hours",
            "resting_heart_rate",
            "hr_variability_sdnn"
        ]

        let now = Date()
        guard let start60 = Calendar.current.date(byAdding: .day, value: -60, to: now) else {
            completion(.failure(NSError(domain: "csv", code: -1, userInfo: [NSLocalizedDescriptionKey: "Date math failed"])));
            return
        }

        // Rows are appended from many HealthKit callbacks concurrently; guard with a serial queue.
        let rowsQueue = DispatchQueue(label: "HealthCSVExporter.rows")
        var rows: [String] = ["user_id,timestamp,metric_type,metric_value,unit,source,timezone,utc_offset_min,created_at"]
        func appendRow(_ line: String) {
            rowsQueue.sync { rows.append(line) }
        }

        let baseSpecs = MetricSpec.defaultSpecs().filter { requestedMetrics.isEmpty ? true : requestedMetrics.contains($0.name) }
        let qtyTypes: Set<HKQuantityType> = Set(baseSpecs.compactMap { $0.quantityType })

        // Export quantity values in the user's preferred units (matches Apple Health display settings).
        healthStore.preferredUnits(for: qtyTypes) { preferredUnits, _ in
            let specs = baseSpecs.map { spec in
                if let qt = spec.quantityType, let u = preferredUnits[qt] {
                    return spec.withUnit(u)
                }
                return spec
            }

            // Also apply preferred units to derived workout values (distance/energy).
            let distanceUnit: HKUnit = {
                if let qt = HKQuantityType.quantityType(forIdentifier: .distanceWalkingRunning),
                   let u = preferredUnits[qt] {
                    return u
                }
                return HKUnit.meterUnit(with: .kilo) // km fallback
            }()
            let distanceUnitLabel = cleanedHKUnitLabel(distanceUnit)

            let energyUnit: HKUnit = {
                if let qt = HKQuantityType.quantityType(forIdentifier: .activeEnergyBurned),
                   let u = preferredUnits[qt] {
                    return u
                }
                return HKUnit.kilocalorie() // kcal fallback
            }()
            let energyUnitLabel = cleanedHKUnitLabel(energyUnit)

            let group = DispatchGroup()
            let encounteredError: Error? = nil

            for spec in specs {
            // Decide bucket size per metric
            let interval: DateComponents = {
                if dailyNames.contains(spec.name) {
                    return DateComponents(day: 1)
                }
                if minutelyNames.contains(spec.name) {
                    return DateComponents(minute: 1)
                }
                return DateComponents(hour: 1)
            }()

            // Single window: last 60 days
            group.enter()
            queryQuantityOrCategory(
                healthStore: healthStore,
                spec: spec,
                start: start60,
                end: now,
                interval: interval,
                aggregation: spec.aggregation
            ) { result in
                switch result {
                case .success(let points):
                    points.forEach { p in
                        // For metrics, prefer on-device timezone history only when it is actually known
                        // for that historical timestamp. If we don't know (e.g. first run after traveling
                        // or after reinstall), leave blank rather than incorrectly stamping everything
                        // with the current timezone.
                        let tz = workoutTimezoneFromGeoHistory(for: p.timestamp)
                        let tzName = csvField(tz.tzName)
                        let offsetMin = tz.utcOffsetMin.map(String.init) ?? ""
                        let line = "\(userId),\(iso.string(from: p.timestamp)),\(spec.name),\(formatValue(p.value)),\(spec.unitLabel),\(p.source),\(tzName),\(offsetMin),\(createdAt)"
                        appendRow(line)
                    }
                case .failure(let error):
                    // Non-fatal: ignore per-metric authorization or data errors
                    print("[HealthCSVExporter] window60 '\(spec.name)' error: \(error.localizedDescription)")
                }
                group.leave()
            }
            }

        // MARK: - Workouts + simple events
            group.enter()
            let workoutType = HKObjectType.workoutType()
            let workoutPredicate = HKQuery.predicateForSamples(withStart: start60, end: now, options: .strictStartDate)
            let sort = NSSortDescriptor(key: HKSampleSortIdentifierStartDate, ascending: true)
            let workoutQuery = HKSampleQuery(sampleType: workoutType, predicate: workoutPredicate, limit: HKObjectQueryNoLimit, sortDescriptors: [sort]) { _, samples, error in
                if let error = error {
                    // Non-fatal: skip workouts if unauthorized
                    print("[HealthCSVExporter] workouts query error: \(error.localizedDescription)")
                    group.leave()
                    return
                }
                let workouts = (samples as? [HKWorkout]) ?? []
                if samples == nil {
                    print("[HealthCSVExporter] workouts query returned nil samples (treating as 0 workouts)")
                }
                let inner = DispatchGroup()
                for w in workouts {
                    inner.enter()
                    Self.workoutKilocalories(healthStore: healthStore, workout: w) { kcal in
                        let ts = iso.string(from: w.startDate)
                        let typeLabel = Self.workoutActivityName(w.workoutActivityType)
                        let distOut = w.totalDistance?.doubleValue(for: distanceUnit) ?? 0.0
                        // Keep km for heuristic thresholds regardless of output unit
                        let distKmForHeuristics = (w.totalDistance?.doubleValue(for: HKUnit.meter()) ?? 0.0) / 1000.0
                        let durMin = w.duration / 60.0

                        let energyOut = HKQuantity(unit: HKUnit.kilocalorie(), doubleValue: kcal).doubleValue(for: energyUnit)

                        // Infer timezone from workout route coordinates when available; otherwise fall back to our geo tz history.
                        Self.workoutTimezoneFromRoute(healthStore: healthStore, workout: w) { routeTzName, routeOff in
                            let workoutDate = w.startDate
                            let tz = routeTzName != nil
                                ? (tzName: routeTzName, utcOffsetMin: routeOff)
                                : workoutTimezoneFromGeoHistory(for: workoutDate)
                            let tzName = csvField(tz.tzName)
                            let offsetMin = tz.utcOffsetMin.map(String.init) ?? ""

                            appendRow("\(userId),\(ts),workout_distance_km,\(formatValue(distOut)),\(distanceUnitLabel),\(typeLabel),\(tzName),\(offsetMin),\(createdAt)")
                            appendRow("\(userId),\(ts),workout_duration_min,\(formatValue(durMin)),min,\(typeLabel),\(tzName),\(offsetMin),\(createdAt)")
                            appendRow("\(userId),\(ts),workout_energy_kcal,\(formatValue(energyOut)),\(energyUnitLabel),\(typeLabel),\(tzName),\(offsetMin),\(createdAt)")

                            // Simple events
                            if distKmForHeuristics >= 10.0 {
                                appendRow("\(userId),\(ts),event_long_run_km,\(formatValue(distOut)),\(distanceUnitLabel),\(typeLabel),\(tzName),\(offsetMin),\(createdAt)")
                            }
                            if kcal >= 800.0 || durMin >= 60.0 {
                                appendRow("\(userId),\(ts),event_hard_workout,1,count,\(typeLabel),\(tzName),\(offsetMin),\(createdAt)")
                            }
                            inner.leave()
                        }
                    }
                }
                inner.notify(queue: .global(qos: .utility)) {
                    group.leave()
                }
            }
            healthStore.execute(workoutQuery)

            group.notify(queue: .main) {
                if let error = encounteredError {
                    completion(.failure(error))
                    return
                }
                let data = rowsQueue.sync { rows.joined(separator: "\n").data(using: .utf8) ?? Data() }
                completion(.success(data))
            }
        } // preferredUnits callback
    }

    // MARK: - Delta Exporter (hourly only within [start, end])
    static func generateDeltaCSV(for userId: String,
                                 start: Date,
                                 end: Date,
                                 metrics requestedMetrics: [String] = [],
                                 minuteResolution: Bool = false,
                                 completion: @escaping (Result<Data, Error>) -> Void) {
        let healthStore = HKHealthStore()
        let createdAt = ISO8601DateFormatter().string(from: Date())
        let iso = ISO8601DateFormatter()
        // Same cadence sets as initial export
        let minutelyNames: Set<String> = [
            "heart_rate",
            "steps",
            "active_energy_burned",
            "walking_speed",
            "distance_walking_running_km",
            "distance_cycling_km",
            "distance_swimming_km",
            "active_time_minutes"
        ]
        let dailyNames: Set<String> = [
            "sleep_hours",
            "resting_heart_rate",
            "hr_variability_sdnn"
        ]

        let rowsQueue = DispatchQueue(label: "HealthCSVExporter.delta.rows")
        var rows: [String] = ["user_id,timestamp,metric_type,metric_value,unit,source,timezone,utc_offset_min,created_at"]
        func appendRow(_ line: String) {
            rowsQueue.sync { rows.append(line) }
        }

        let baseSpecs = MetricSpec.defaultSpecs().filter { requestedMetrics.isEmpty ? true : requestedMetrics.contains($0.name) }
        let qtyTypes: Set<HKQuantityType> = Set(baseSpecs.compactMap { $0.quantityType })

        healthStore.preferredUnits(for: qtyTypes) { preferredUnits, _ in
            let specs = baseSpecs.map { spec in
                if let qt = spec.quantityType, let u = preferredUnits[qt] {
                    return spec.withUnit(u)
                }
                return spec
            }

            let distanceUnit: HKUnit = {
                if let qt = HKQuantityType.quantityType(forIdentifier: .distanceWalkingRunning),
                   let u = preferredUnits[qt] {
                    return u
                }
                return HKUnit.meterUnit(with: .kilo)
            }()
            let distanceUnitLabel = cleanedHKUnitLabel(distanceUnit)

            let energyUnit: HKUnit = {
                if let qt = HKQuantityType.quantityType(forIdentifier: .activeEnergyBurned),
                   let u = preferredUnits[qt] {
                    return u
                }
                return HKUnit.kilocalorie()
            }()
            let energyUnitLabel = cleanedHKUnitLabel(energyUnit)

            let group = DispatchGroup()
            let encounteredError: Error? = nil

            for spec in specs {
                group.enter()
                // Choose interval per metric; allow minuteResolution to force minute for short windows
                let interval: DateComponents = {
                    if dailyNames.contains(spec.name) {
                        return DateComponents(day: 1)
                    }
                    if minuteResolution || minutelyNames.contains(spec.name) {
                        return DateComponents(minute: 1)
                    }
                    return DateComponents(hour: 1)
                }()
                queryQuantityOrCategory(
                    healthStore: healthStore,
                    spec: spec,
                    start: start,
                    end: end,
                    interval: interval,
                    aggregation: spec.aggregation
                ) { result in
                    switch result {
                    case .success(let points):
                        points.forEach { p in
                            let tz = workoutTimezoneFromGeoHistory(for: p.timestamp)
                            let tzName = csvField(tz.tzName)
                            let offsetMin = tz.utcOffsetMin.map(String.init) ?? ""
                            let line = "\(userId),\(iso.string(from: p.timestamp)),\(spec.name),\(formatValue(p.value)),\(spec.unitLabel),\(p.source),\(tzName),\(offsetMin),\(createdAt)"
                            appendRow(line)
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
                if let error = error {
                    print("[HealthCSVExporter] delta workouts query error: \(error.localizedDescription)")
                    group.leave()
                    return
                }
                let workouts = (samples as? [HKWorkout]) ?? []
                let inner = DispatchGroup()
                for w in workouts {
                    inner.enter()
                    Self.workoutKilocalories(healthStore: healthStore, workout: w) { kcal in
                        let ts = iso.string(from: w.startDate)
                        let typeLabel = Self.workoutActivityName(w.workoutActivityType)
                        let distOut = w.totalDistance?.doubleValue(for: distanceUnit) ?? 0.0
                        let distKmForHeuristics = (w.totalDistance?.doubleValue(for: HKUnit.meter()) ?? 0.0) / 1000.0
                        let durMin = w.duration / 60.0
                        let workoutDate = w.startDate
                        // Route-based tz inference is async; use geo history fallback for delta exporter workouts to avoid
                        // delaying the export (delta windows are short and often lack routes for indoor workouts).
                        let tz = workoutTimezoneFromGeoHistory(for: workoutDate)
                        let tzName = csvField(tz.tzName)
                        let offsetMin = tz.utcOffsetMin.map(String.init) ?? ""

                        let energyOut = HKQuantity(unit: HKUnit.kilocalorie(), doubleValue: kcal).doubleValue(for: energyUnit)

                        appendRow("\(userId),\(ts),workout_distance_km,\(formatValue(distOut)),\(distanceUnitLabel),\(typeLabel),\(tzName),\(offsetMin),\(createdAt)")
                        appendRow("\(userId),\(ts),workout_duration_min,\(formatValue(durMin)),min,\(typeLabel),\(tzName),\(offsetMin),\(createdAt)")
                        appendRow("\(userId),\(ts),workout_energy_kcal,\(formatValue(energyOut)),\(energyUnitLabel),\(typeLabel),\(tzName),\(offsetMin),\(createdAt)")
                        if distKmForHeuristics >= 10.0 {
                            appendRow("\(userId),\(ts),event_long_run_km,\(formatValue(distOut)),\(distanceUnitLabel),\(typeLabel),\(tzName),\(offsetMin),\(createdAt)")
                        }
                        if kcal >= 800.0 || durMin >= 60.0 {
                            appendRow("\(userId),\(ts),event_hard_workout,1,count,\(typeLabel),\(tzName),\(offsetMin),\(createdAt)")
                        }
                        inner.leave()
                    }
                }
                inner.notify(queue: .global(qos: .utility)) {
                    group.leave()
                }
            }
            healthStore.execute(workoutQuery)

            group.notify(queue: .main) {
                if let error = encounteredError {
                    completion(.failure(error))
                    return
                }
                let data = rowsQueue.sync { rows.joined(separator: "\n").data(using: .utf8) ?? Data() }
                completion(.success(data))
            }
        } // preferredUnits callback
    }

    // MARK: - Workout helpers
    private static func workoutKilocalories(healthStore: HKHealthStore, workout: HKWorkout, completion: @escaping (Double) -> Void) {
        // Preferred: workout-provided statistics (non-deprecated)
        if let qt = HKQuantityType.quantityType(forIdentifier: .activeEnergyBurned),
           let sum = workout.statistics(for: qt)?.sumQuantity() {
            completion(sum.doubleValue(for: HKUnit.kilocalorie()))
            return
        }

        // Fallback: sum activeEnergyBurned over the workout time window.
        guard let qt = HKObjectType.quantityType(forIdentifier: .activeEnergyBurned) else {
            completion(0.0)
            return
        }
        let predicate = HKQuery.predicateForSamples(withStart: workout.startDate, end: workout.endDate, options: .strictStartDate)
        let q = HKStatisticsQuery(quantityType: qt, quantitySamplePredicate: predicate, options: .cumulativeSum) { _, result, _ in
            let kcal = result?.sumQuantity()?.doubleValue(for: HKUnit.kilocalorie()) ?? 0.0
            completion(kcal)
        }
        healthStore.execute(q)
    }


    private struct MetricSpec {
        enum Aggregation { case average, sum }
        let name: String
        let quantityType: HKQuantityType?
        let categoryType: HKCategoryType?
        let unit: HKUnit?
        let unitLabel: String
        let aggregation: Aggregation

        func withUnit(_ preferredUnit: HKUnit) -> MetricSpec {
            // Export values in the user's preferred HealthKit units and stamp the exact unit string into CSV.
            MetricSpec(
                name: name,
                quantityType: quantityType,
                categoryType: categoryType,
                unit: preferredUnit,
                unitLabel: HealthCSVExporter.cleanedHKUnitLabel(preferredUnit),
                aggregation: aggregation
            )
        }

        static func defaultSpecs() -> [MetricSpec] {
            var list: [MetricSpec] = []
            func q(_ id: HKQuantityTypeIdentifier) -> HKQuantityType { HKObjectType.quantityType(forIdentifier: id)! }
            func c(_ id: HKCategoryTypeIdentifier) -> HKCategoryType { HKObjectType.categoryType(forIdentifier: id)! }

            list.append(MetricSpec(name: "heart_rate",               quantityType: q(.heartRate),               categoryType: nil, unit: HKUnit.count().unitDivided(by: .minute()), unitLabel: "bpm",   aggregation: .average))
            list.append(MetricSpec(name: "resting_heart_rate",       quantityType: q(.restingHeartRate),        categoryType: nil, unit: HKUnit.count().unitDivided(by: .minute()), unitLabel: "bpm",   aggregation: .average))
            list.append(MetricSpec(name: "walking_hr_avg",           quantityType: q(.walkingHeartRateAverage), categoryType: nil, unit: HKUnit.count().unitDivided(by: .minute()), unitLabel: "bpm",   aggregation: .average))
            list.append(MetricSpec(name: "hr_variability_sdnn",      quantityType: q(.heartRateVariabilitySDNN),categoryType: nil, unit: HKUnit.secondUnit(with: .milli),          unitLabel: "ms",    aggregation: .average))
            list.append(MetricSpec(name: "steps",                    quantityType: q(.stepCount),              categoryType: nil, unit: HKUnit.count(),                          unitLabel: "count", aggregation: .sum))
            list.append(MetricSpec(name: "walking_speed",            quantityType: q(.walkingSpeed),           categoryType: nil, unit: HKUnit.meter().unitDivided(by: .second()), unitLabel: "m_per_s",aggregation: .average))
            list.append(MetricSpec(name: "vo2_max",                  quantityType: q(.vo2Max),                 categoryType: nil, unit: HKUnit(from: "ml/(kg*min)"),             unitLabel: "ml_per_kg_min", aggregation: .average))
            list.append(MetricSpec(name: "active_energy_burned",     quantityType: q(.activeEnergyBurned),     categoryType: nil, unit: HKUnit.kilocalorie(),                     unitLabel: "kcal",  aggregation: .sum))
            // Standalone distances to support inferred sessions when no HKWorkout exists
            list.append(MetricSpec(name: "distance_walking_running_km", quantityType: q(.distanceWalkingRunning), categoryType: nil, unit: HKUnit.meterUnit(with: .kilo), unitLabel: "km", aggregation: .sum))
            if let qc = HKObjectType.quantityType(forIdentifier: .distanceCycling) {
                list.append(MetricSpec(name: "distance_cycling_km", quantityType: qc, categoryType: nil, unit: HKUnit.meterUnit(with: .kilo), unitLabel: "km", aggregation: .sum))
            }
            if let qs = HKObjectType.quantityType(forIdentifier: .distanceSwimming) {
                list.append(MetricSpec(name: "distance_swimming_km", quantityType: qs, categoryType: nil, unit: HKUnit.meterUnit(with: .kilo), unitLabel: "km", aggregation: .sum))
            }
            list.append(MetricSpec(name: "dietary_water",            quantityType: q(.dietaryWater),           categoryType: nil, unit: HKUnit.liter(),                           unitLabel: "L",     aggregation: .sum))
            list.append(MetricSpec(name: "body_mass",                quantityType: q(.bodyMass),               categoryType: nil, unit: HKUnit.gramUnit(with: .kilo),             unitLabel: "kg",    aggregation: .average))
            list.append(MetricSpec(name: "body_mass_index",          quantityType: q(.bodyMassIndex),          categoryType: nil, unit: HKUnit.count(),                          unitLabel: "bmi",   aggregation: .average))
            list.append(MetricSpec(name: "blood_glucose",            quantityType: q(.bloodGlucose),           categoryType: nil, unit: HKUnit(from: "mg/dL"),                    unitLabel: "mg_dL", aggregation: .average))
            list.append(MetricSpec(name: "oxygen_saturation",        quantityType: q(.oxygenSaturation),       categoryType: nil, unit: HKUnit.percent(),                          unitLabel: "percent",aggregation: .average))
            list.append(MetricSpec(name: "blood_pressure_systolic",  quantityType: q(.bloodPressureSystolic),  categoryType: nil, unit: HKUnit.millimeterOfMercury(),             unitLabel: "mmHg",  aggregation: .average))
            list.append(MetricSpec(name: "blood_pressure_diastolic", quantityType: q(.bloodPressureDiastolic), categoryType: nil, unit: HKUnit.millimeterOfMercury(),             unitLabel: "mmHg",  aggregation: .average))
            list.append(MetricSpec(name: "respiratory_rate",         quantityType: q(.respiratoryRate),        categoryType: nil, unit: HKUnit.count().unitDivided(by: .minute()), unitLabel: "breaths_per_min", aggregation: .average))
            list.append(MetricSpec(name: "body_temperature",         quantityType: q(.bodyTemperature),        categoryType: nil, unit: HKUnit.degreeCelsius(),                   unitLabel: "degC",  aggregation: .average))
            list.append(MetricSpec(name: "mindfulness_minutes",      quantityType: nil,                        categoryType: c(.mindfulSession), unit: HKUnit.minute(),            unitLabel: "min",   aggregation: .sum))
            list.append(MetricSpec(name: "sleep_hours",              quantityType: nil,                        categoryType: c(.sleepAnalysis),  unit: HKUnit.hour(),              unitLabel: "hours", aggregation: .sum))
            list.append(MetricSpec(name: "active_time_minutes",      quantityType: q(.appleExerciseTime),      categoryType: nil, unit: HKUnit.minute(),                           unitLabel: "min",   aggregation: .sum))
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
            queryQuantity(healthStore: healthStore, quantityType: qt, unit: spec.unit, start: start, end: end, interval: interval, aggregation: aggregation, completion: completion)
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
                                      completion: @escaping (Result<[DataPoint], Error>) -> Void) {
        let predicate = HKQuery.predicateForSamples(withStart: start, end: end, options: .strictStartDate)
        // Use a stable anchor at local midnight to ensure bucket boundaries are identical across exports
        let anchorDate = Calendar.current.startOfDay(for: Date())
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
                    // Always include zero-valued sum buckets
                    maybeValue = v
                }
                if aggregation == .average, let avgQ = stats.averageQuantity() {
                    let v = avgQ.doubleValue(for: unit ?? HKUnit.count())
                    maybeValue = v
                }
                if var value = maybeValue {
                    // Oxygen saturation is commonly represented as a fraction (0..1). Convert to percentage points (0..100)
                    // for easier interpretation and alignment with how the Health app presents it.
                    if quantityType.identifier == HKQuantityTypeIdentifier.oxygenSaturation.rawValue {
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
