//
//  HealthMetricMapper.swift
//  HealthPredictor
//
//  Created by Stephan  on 09.06.2025.
//

import Foundation
import HealthKit

struct HealthMetricMapper {

    static let quantitySubtagToType: [String: HKQuantityTypeIdentifier] = [
        "HRV": .heartRateVariabilitySDNN,
        "Heart Rate": .heartRate,
        "Resting HR": .restingHeartRate,
        "Walking HR": .walkingHeartRateAverage,
        "Blood Pressure": .bloodPressureSystolic,
        "Step Count": .stepCount,
        "Walking Speed": .walkingSpeed,
        "VO2 Max": .vo2Max,
        "Active Energy": .activeEnergyBurned,
        "Hydration": .dietaryWater,
        "Weight": .bodyMass,
        "BMI": .bodyMassIndex,
        "Blood Glucose": .bloodGlucose,
        "Oxygen Saturation": .oxygenSaturation,
        "Body Fat": .bodyFatPercentage,
        "Muscle Mass": .leanBodyMass,
        "Basal Energy": .basalEnergyBurned,
        "Flights Climbed": .flightsClimbed,
        "Distance Walking": .distanceWalkingRunning,
        "Exercise Minutes": .appleExerciseTime,
        "Stand Hours": .appleStandTime,
        "Respiratory Rate": .respiratoryRate,
        "Body Temperature": .bodyTemperature,
        "Blood Pressure Diastolic": .bloodPressureDiastolic
    ]

    static let categorySubtagToType: [String: HKCategoryTypeIdentifier] = [
        "Sleep Duration": .sleepAnalysis,
        "Mindfulness Minutes": .mindfulSession
    ]

    static let subtagToUnit: [String: String] = [
        "Heart Rate": "count/min",
        "Resting HR": "count/min",
        "Walking HR": "count/min",
        "HRV": "ms",
        "Step Count": "count",
        "Walking Speed": "m/s",
        "VO2 Max": "ml/(kg*min)",
        "Active Energy": "kcal",
        "Hydration": "mL",
        "Blood Glucose": "mg/dL",
        "Oxygen Saturation": "%",
        "Weight": "kg",
        "BMI": "",
        "Blood Pressure": "mmHg",
        "Mindfulness Minutes": "min",
        "Sleep Duration": "hours",
        "Body Fat": "%",
        "Muscle Mass": "kg",
        "Basal Energy": "kcal",
        "Flights Climbed": "count",
        "Distance Walking": "m",
        "Exercise Minutes": "min",
        "Stand Hours": "hr",
        "Respiratory Rate": "count/min",
        "Body Temperature": "degC",
        "Blood Pressure Diastolic": "mmHg"
    ]

    static func quantityType(for subtag: String) -> HKQuantityTypeIdentifier? {
        quantitySubtagToType[subtag]
    }

    static func categoryType(for subtag: String) -> HKCategoryTypeIdentifier? {
        categorySubtagToType[subtag]
    }

    static func statisticsOption(for subtag: String) -> HKStatisticsOptions {
        switch subtag {
        case "Step Count", "Active Energy", "Hydration", "Flights Climbed", "Distance Walking", "Exercise Minutes", "Stand Hours", "Basal Energy":
            return .cumulativeSum
        default:
            return .discreteAverage
        }
    }

    static func unit(for subtag: String) -> String {
        subtagToUnit[subtag] ?? ""
    }
}
