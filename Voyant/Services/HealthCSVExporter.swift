import Foundation
import HealthKit

struct HealthCSVExporter {

    private static func csvField(_ s: String?) -> String {
        // Minimal CSV quoting to safely embed fields that may contain commas/quotes (e.g., JSON blobs).
        let raw = (s ?? "").replacingOccurrences(of: "\n", with: " ").replacingOccurrences(of: "\r", with: " ")
        if raw.isEmpty { return "" }
        if raw.contains(",") || raw.contains("\"") {
            let escaped = raw.replacingOccurrences(of: "\"", with: "\"\"")
            return "\"\(escaped)\""
        }
        return raw
    }

    // (Legacy unit normalization removed; mirroring stores canonical values in fixed units.)

    private struct Place {
        let country: String?
        let region: String?
        let city: String?
    }

    private static func placeFields(_ place: Place?) -> (country: String, region: String, city: String) {
        return (
            csvField(place?.country),
            csvField(place?.region),
            csvField(place?.city)
        )
    }

    private static func contextForTimestamp(_ date: Date) -> (tzName: String?, utcOffsetMin: Int?, place: Place?) {
        // Prefer geo *geocoded* entries; don't let Geo's seed_device_tz placeholder override TZ-history fallback.
        if let geo = GeoTimezoneHistoryService.shared.geocodedEntryIfKnown(for: date),
           let tz = TimeZone(identifier: geo.tzName) {
            let p = geo.place.map { Place(country: $0.country, region: $0.region, city: $0.city) }
            return (tz.identifier, tz.secondsFromGMT(for: date) / 60, p)
        }
        if let name = TimezoneHistoryService.shared.tzNameIfKnown(for: date),
           let tz = TimeZone(identifier: name) {
            return (tz.identifier, tz.secondsFromGMT(for: date) / 60, nil)
        }
        return (nil, nil, nil)
    }

    // MARK: - Raw-sample mirroring only.

    private struct MetricSpec {
        enum Aggregation { case average, sum }
        let name: String
        let quantityType: HKQuantityType?
        let categoryType: HKCategoryType?
        let unit: HKUnit?
        let unitLabel: String
        let aggregation: Aggregation

        // Legacy preferred-unit exporter removed; mirroring stores canonical values in fixed units.

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

    // MARK: - HealthKit Mirror (raw samples + deletions)
    private static func mirrorHeaderRow() -> String {
        // Keep the original columns first for backwards-compat with existing backend ingest,
        // then append mirror columns (ignored by older backends).
        return [
            "user_id",
            "timestamp",
            "metric_type",
            "metric_value",
            "unit",
            "source",
            "timezone",
            "utc_offset_min",
            "place_country",
            "place_region",
            "place_city",
            "created_at",
            "op",
            "hk_uuid",
            "end_ts",
            "hk_source_bundle_id",
            "hk_source_name",
            "hk_source_version",
            "hk_metadata"
        ].joined(separator: ",")
    }

    private static func jsonStringForMetadata(_ meta: [String: Any]?) -> String? {
        guard let meta = meta, !meta.isEmpty else { return nil }

        // HealthKit metadata values are heterogeneous; keep only JSON-safe primitives.
        var out: [String: Any] = [:]
        for (k, v) in meta {
            switch v {
            case let s as String:
                out[k] = s
            case let n as NSNumber:
                out[k] = n
            case let d as Date:
                out[k] = ISO8601DateFormatter().string(from: d)
            default:
                // Skip unsupported types (e.g., HKWorkoutActivityType wrappers, CLLocation, etc.)
                continue
            }
        }
        guard !out.isEmpty,
              JSONSerialization.isValidJSONObject(out),
              let data = try? JSONSerialization.data(withJSONObject: out, options: []) else {
            return nil
        }
        return String(data: data, encoding: .utf8)
    }

    private static func specsByIdentifier() -> [String: MetricSpec] {
        var map: [String: MetricSpec] = [:]
        for s in MetricSpec.defaultSpecs() {
            if let qt = s.quantityType {
                map[qt.identifier] = s
            }
            if let ct = s.categoryType {
                map[ct.identifier] = s
            }
        }
        return map
    }

    // Base workout row types mirrored into backend's health_events table.
    // Derived flags (event_long_run_km / event_hard_workout) are computed server-side.
    private static func workoutBaseEventTypes() -> [String] {
        return [
            "workout_distance_km",
            "workout_duration_min",
            "workout_energy_kcal",
        ]
    }

    private static func workoutTypeLabel(_ workout: HKWorkout) -> String {
        return workoutActivityName(workout.workoutActivityType)
    }

    private static func workoutRows(userId: String,
                                    workout: HKWorkout,
                                    createdAt: String,
                                    iso: ISO8601DateFormatter) -> [String] {
        let start = workout.startDate
        let end = workout.endDate

        let (tzNameOpt, utcOffsetMinOpt, placeOpt) = contextForTimestamp(start)
        let tzName = csvField(tzNameOpt)
        let offsetMin = utcOffsetMinOpt.map(String.init) ?? ""
        let place = placeFields(placeOpt)

        let sr = workout.sourceRevision
        let sourceName = sr.source.name
        let bundleId = sr.source.bundleIdentifier
        let version = sr.version

        let metaJson = csvField(jsonStringForMetadata(workout.metadata))

        let workoutUUID = workout.uuid.uuidString
        let endTs = (end != start) ? iso.string(from: end) : ""

        let typeLabel = workoutTypeLabel(workout)

        let distKm = (workout.totalDistance?.doubleValue(for: HKUnit.meter()) ?? 0.0) / 1000.0
        let durMin = workout.duration / 60.0
        let kcal: Double = {
            if let qt = HKQuantityType.quantityType(forIdentifier: .activeEnergyBurned),
               let sum = workout.statistics(for: qt)?.sumQuantity() {
                return sum.doubleValue(for: HKUnit.kilocalorie())
            }
            // Avoid deprecated `totalEnergyBurned` on newer SDKs; prefer statistics when available.
            if let qt = HKQuantityType.quantityType(forIdentifier: .activeEnergyBurned),
               let sum = workout.statistics(for: qt)?.sumQuantity() {
                return sum.doubleValue(for: HKUnit.kilocalorie())
            }
            return 0.0
        }()

        func row(eventType: String, value: Double, unit: String) -> String {
            // hk_uuid is made unique per derived row so the backend can tombstone by hk_uuid.
            let hkUUID = "\(workoutUUID)|\(eventType)"
            return [
                userId,
                iso.string(from: start),
                eventType,
                formatValue(value),
                unit,
                csvField(typeLabel), // keep "source" stable like before (running/strength/etc)
                tzName,
                offsetMin,
                place.country,
                place.region,
                place.city,
                createdAt,
                "upsert",
                hkUUID,
                endTs,
                csvField(bundleId),
                csvField(sourceName),
                csvField(version),
                metaJson
            ].joined(separator: ",")
        }

        return [
            row(eventType: "workout_distance_km", value: distKm, unit: "km"),
            row(eventType: "workout_duration_min", value: durMin, unit: "min"),
            row(eventType: "workout_energy_kcal", value: kcal, unit: "kcal"),
        ]
    }

    private static func mirrorRowForSample(userId: String,
                                           sample: HKSample,
                                                spec: MetricSpec,
                                           createdAt: String,
                                           iso: ISO8601DateFormatter) -> String? {
        let start = sample.startDate
        let end = sample.endDate

        let (tzNameOpt, utcOffsetMinOpt, placeOpt) = contextForTimestamp(start)
        let tzName = csvField(tzNameOpt)
        let offsetMin = utcOffsetMinOpt.map(String.init) ?? ""
        let place = placeFields(placeOpt)

        let sr = sample.sourceRevision
        let sourceName = sr.source.name
        let bundleId = sr.source.bundleIdentifier
        let version = sr.version

        let metaJson = csvField(jsonStringForMetadata(sample.metadata))

        let hkUUID = sample.uuid.uuidString

        var value: Double?
        var unitLabel: String = spec.unitLabel
        var endTs: String = ""

        if let qs = sample as? HKQuantitySample, let unit = spec.unit {
            value = qs.quantity.doubleValue(for: unit)
            // Oxygen saturation is commonly represented as a fraction (0..1). Convert to percentage points (0..100)
            if qs.quantityType.identifier == HKQuantityTypeIdentifier.oxygenSaturation.rawValue {
                value = (value ?? 0) * 100.0
                unitLabel = "percent"
            }
            if end != start {
                endTs = iso.string(from: end)
            }
        } else if sample is HKCategorySample {
            // Represent category samples as a duration in the spec's unit.
            endTs = iso.string(from: end)
            let seconds = max(0, end.timeIntervalSince(start))
            if unitLabel == "hours" {
                value = seconds / 3600.0
            } else if unitLabel == "min" {
                value = seconds / 60.0
            } else {
                // Fallback: seconds as minutes.
                value = seconds / 60.0
                unitLabel = "min"
            }
        } else {
            return nil
        }

        guard let v = value, v.isFinite else { return nil }

        let line = [
            userId,
            iso.string(from: start),
            spec.name,
            formatValue(v),
            unitLabel,
            csvField(sourceName),
            tzName,
            offsetMin,
            place.country,
            place.region,
            place.city,
            createdAt,
            "upsert",
            hkUUID,
            endTs,
            csvField(bundleId),
            csvField(sourceName),
            csvField(version),
            metaJson
        ].joined(separator: ",")
        return line
    }

    static func generateMirrorDeltaCSV(for userId: String,
                                       samples: [HKSample],
                                       deleted: [HKDeletedObject],
                                       completion: @escaping (Result<Data, Error>) -> Void) {
        let createdAt = ISO8601DateFormatter().string(from: Date())
        let iso = ISO8601DateFormatter()
        let rowsQueue = DispatchQueue(label: "HealthCSVExporter.mirror.delta.rows")
        var rows: [String] = [mirrorHeaderRow()]
        func appendRow(_ line: String) { rowsQueue.sync { rows.append(line) } }

        let specMap = specsByIdentifier()

        for s in samples {
            if let w = s as? HKWorkout {
                for line in workoutRows(userId: userId, workout: w, createdAt: createdAt, iso: iso) {
                    appendRow(line)
                }
                continue
            }
            let key: String
            if let qs = s as? HKQuantitySample {
                key = qs.quantityType.identifier
            } else if let cs = s as? HKCategorySample {
                key = cs.categoryType.identifier
            } else {
                continue
            }
            guard let spec = specMap[key] else { continue }
            if let line = mirrorRowForSample(userId: userId, sample: s, spec: spec, createdAt: createdAt, iso: iso) {
                appendRow(line)
            }
        }

        for d in deleted {
            // If this deleted object corresponds to a workout, also emit deletes for the derived rows.
            // (HealthKit only gives the workout UUID, but our health_events rows use workoutUUID|event_type.)
            let workoutUUID = d.uuid.uuidString
            for et in workoutBaseEventTypes() {
                let hkUUID = "\(workoutUUID)|\(et)"
                let line = [
                    userId,
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    createdAt,
                    "delete",
                    hkUUID,
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    ""
                ].joined(separator: ",")
                appendRow(line)
            }
            // Delete tombstone row: uuid only. Backend will mark deleted_at and widen rollup window by uuid lookup.
            let line = [
                userId,
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                createdAt,
                "delete",
                d.uuid.uuidString,
                "",
                "",
                "",
                "",
                "",
                "",
                ""
            ].joined(separator: ",")
            appendRow(line)
        }

        let data = rowsQueue.sync { rows.joined(separator: "\n").data(using: .utf8) ?? Data() }
        completion(.success(data))
    }

    static func generateMirrorCSV(for userId: String,
                                   start: Date,
                                   end: Date,
                                  completion: @escaping (Result<Data, Error>) -> Void) {
        let healthStore = HKHealthStore()
        let createdAt = ISO8601DateFormatter().string(from: Date())
        let iso = ISO8601DateFormatter()
        let rowsQueue = DispatchQueue(label: "HealthCSVExporter.mirror.seed.rows")
        var rows: [String] = [mirrorHeaderRow()]
        func appendRow(_ line: String) { rowsQueue.sync { rows.append(line) } }

        let specMap = specsByIdentifier()
        let specs = MetricSpec.defaultSpecs()
        let group = DispatchGroup()

        let predicate = HKQuery.predicateForSamples(withStart: start, end: end, options: .strictStartDate)
        let sort = NSSortDescriptor(key: HKSampleSortIdentifierStartDate, ascending: true)

        for spec in specs {
            if let qt = spec.quantityType {
                group.enter()
                let query = HKSampleQuery(sampleType: qt, predicate: predicate, limit: HKObjectQueryNoLimit, sortDescriptors: [sort]) { _, samples, error in
                    defer { group.leave() }
                    if error != nil { return }
                    let ss = samples ?? []
                    for s in ss {
                        let key = (s as? HKQuantitySample)?.quantityType.identifier ?? qt.identifier
                        guard let eff = specMap[key] else { continue }
                        if let line = mirrorRowForSample(userId: userId, sample: s, spec: eff, createdAt: createdAt, iso: iso) {
                            appendRow(line)
                        }
                    }
                }
                healthStore.execute(query)
            } else if let ct = spec.categoryType {
                group.enter()
                let query = HKSampleQuery(sampleType: ct, predicate: predicate, limit: HKObjectQueryNoLimit, sortDescriptors: [sort]) { _, samples, error in
                    defer { group.leave() }
                    if error != nil { return }
                    let ss = samples ?? []
                    for s in ss {
                        let key = (s as? HKCategorySample)?.categoryType.identifier ?? ct.identifier
                        guard let eff = specMap[key] else { continue }
                        if let line = mirrorRowForSample(userId: userId, sample: s, spec: eff, createdAt: createdAt, iso: iso) {
                            appendRow(line)
                        }
                    }
                }
                healthStore.execute(query)
            }
        }

        // Workouts within window (mirrored into health_events-style rows)
        group.enter()
        let workoutType = HKObjectType.workoutType()
        let workoutPredicate = HKQuery.predicateForSamples(withStart: start, end: end, options: .strictStartDate)
        let workoutSort = NSSortDescriptor(key: HKSampleSortIdentifierStartDate, ascending: true)
        let workoutQuery = HKSampleQuery(sampleType: workoutType, predicate: workoutPredicate, limit: HKObjectQueryNoLimit, sortDescriptors: [workoutSort]) { _, samples, error in
            defer { group.leave() }
            if error != nil { return }
            let workouts = (samples as? [HKWorkout]) ?? []
            for w in workouts {
                for line in workoutRows(userId: userId, workout: w, createdAt: createdAt, iso: iso) {
                    appendRow(line)
                }
            }
        }
        healthStore.execute(workoutQuery)

        group.notify(queue: .main) {
            let data = rowsQueue.sync { rows.joined(separator: "\n").data(using: .utf8) ?? Data() }
            completion(.success(data))
        }
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
