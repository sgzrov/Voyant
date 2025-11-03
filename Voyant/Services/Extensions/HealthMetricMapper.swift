//
//  HealthMetricMapper.swift
//  HealthPredictor
//
//  Created by Stephan on 03.11.2025.
//

import Foundation
import HealthKit

enum HealthMetricMapper {

    // Public maps used by CSV builder
    static let quantitySubtagToType: [String: HKQuantityTypeIdentifier] = [
        "Resting HR": .restingHeartRate,
        "Walking HR": .walkingHeartRateAverage,
        "Blood Glucose": .bloodGlucose
    ]

    static let categorySubtagToType: [String: HKCategoryTypeIdentifier] = [
        "Sleep Duration": .sleepAnalysis,
        "Mindfulness Minutes": .mindfulSession
    ]

    static func quantityType(for metric: String) -> HKQuantityTypeIdentifier? {
        return quantitySubtagToType[metric]
    }

    static func categoryType(for metric: String) -> HKCategoryTypeIdentifier? {
        return categorySubtagToType[metric]
    }

    static func unit(for metric: String) -> String {
        switch metric {
        case "Resting HR", "Walking HR":
            return "count/min"
        case "Blood Glucose":
            return "mg/dL"
        default:
            return "count"
        }
    }

    static func statisticsOption(for metric: String) -> HKStatisticsOptions {
        switch metric {
        case "Blood Glucose":
            return .discreteAverage
        case "Resting HR", "Walking HR":
            return .discreteAverage
        default:
            return .cumulativeSum
        }
    }
}


