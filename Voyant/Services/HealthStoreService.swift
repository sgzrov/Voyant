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

        let readTypes: Set<HKObjectType> = Set(
            quantityTypes.compactMap { HKObjectType.quantityType(forIdentifier: $0) } +
            categoryTypes.compactMap { HKObjectType.categoryType(forIdentifier: $0) } +
            [HKObjectType.workoutType()]
        )

        healthStore.requestAuthorization(toShare: [], read: readTypes) { success, error in
            completion(success, error)
        }
    }

}
