//
//  HealthStoreService.swift
//  HealthPredictor
//
//  Created by Stephan  on 07.06.2025.
//

import Foundation
import HealthKit

class HealthStoreService {

    static let shared = HealthStoreService()

    private init() {}

    var healthStore: HKHealthStore { return _healthStore }
    private let _healthStore = HKHealthStore()

    private let hkReadGrantedKey = "hk_read_granted_v1"

    /// Cached notion of whether we have *read* authorization. HealthKit does not expose a direct "read authorized?"
    /// API, so we set this after probing a lightweight query post-prompt.
    func isReadGrantedCached() -> Bool {
        return UserDefaults.standard.bool(forKey: hkReadGrantedKey)
    }

    private func setReadGrantedCached(_ granted: Bool) {
        UserDefaults.standard.set(granted, forKey: hkReadGrantedKey)
    }

    /// Probe a lightweight HealthKit query to determine whether reads are authorized.
    /// - Returns true when the query completes without an authorization error (even if it returns 0 samples).
    func probeReadAuthorization(completion: @escaping (Bool) -> Void) {
        guard HKHealthStore.isHealthDataAvailable() else {
            completion(false)
            return
        }

        // Pick a common type we already request read access for.
        guard let qt = HKObjectType.quantityType(forIdentifier: .stepCount) else {
            completion(false)
            return
        }

        let start = Calendar.current.date(byAdding: .day, value: -1, to: Date())
        let pred = HKQuery.predicateForSamples(withStart: start, end: Date(), options: .strictStartDate)
        let q = HKSampleQuery(sampleType: qt, predicate: pred, limit: 1, sortDescriptors: nil) { _, _, error in
            if let e = error as NSError? {
                if e.domain == HKErrorDomain,
                   let code = HKError.Code(rawValue: e.code),
                   code == .errorAuthorizationDenied || code == .errorAuthorizationNotDetermined {
                    completion(false)
                    return
                }
                // Other errors (rare) - treat as not granted for gating.
                completion(false)
                return
            }
            completion(true)
        }
        _healthStore.execute(q)
    }

    func requestAuthorization(completion: @escaping (Bool, Error?) -> Void) {
        guard HKHealthStore.isHealthDataAvailable() else {
            completion(false, nil)
            return
        }

        let quantityTypes: [HKQuantityTypeIdentifier] = [
            .heartRate,
            .restingHeartRate,
            .walkingHeartRateAverage,
            .heartRateVariabilitySDNN,
            .stepCount,
            .walkingSpeed,
            .distanceWalkingRunning,
            .distanceCycling,
            .distanceSwimming,
            .vo2Max,
            .activeEnergyBurned,
            .dietaryWater,
            .bodyMass,
            .bodyMassIndex,
            .bloodGlucose,
            .oxygenSaturation,
            .bloodPressureSystolic,
            .bloodPressureDiastolic,
            .respiratoryRate,
            .bodyTemperature,
            .appleExerciseTime
        ]

        let categoryTypes: [HKCategoryTypeIdentifier] = [
            .sleepAnalysis,
            .mindfulSession
        ]

        var readTypes: [HKObjectType] =
            quantityTypes.compactMap { HKObjectType.quantityType(forIdentifier: $0) } +
            categoryTypes.compactMap { HKObjectType.categoryType(forIdentifier: $0) } +
            [HKObjectType.workoutType()]

        // Needed to read workout routes so we can infer timezone from coordinates when available.
        readTypes.append(HKSeriesType.workoutRoute())

        let readTypesSet: Set<HKObjectType> = Set(readTypes)

        healthStore.requestAuthorization(toShare: [], read: readTypesSet) { success, error in
            // `success` means the request executed; user may still deny. Determine "read granted" via probe.
            self.probeReadAuthorization { granted in
                self.setReadGrantedCached(granted)
                completion(granted, error)
            }
        }
    }

}
