//
//  HealthStoreService.swift
//  HealthPredictor
//
//  Created by Stephan  on 07.06.2025.
//

import Foundation
import HealthKit

class HealthStoreService: HealthStoreServiceProtocol {

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
            .vo2Max,
            .activeEnergyBurned,
            .dietaryWater,
            .bodyMass,
            .bodyMassIndex,
            .bloodGlucose,
            .oxygenSaturation,
            .bloodPressureSystolic,
            .bloodPressureDiastolic
        ]

        let categoryTypes: [HKCategoryTypeIdentifier] = [
            .sleepAnalysis,
            .mindfulSession
        ]

        let readTypes: Set<HKObjectType> = Set(
            quantityTypes.compactMap { HKObjectType.quantityType(forIdentifier: $0) } +
            categoryTypes.compactMap { HKObjectType.categoryType(forIdentifier: $0) }
        )

        healthStore.requestAuthorization(toShare: [], read: readTypes) { success, error in
            completion(success, error)
        }
    }

}
