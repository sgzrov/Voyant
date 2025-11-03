//
//  HealthMetricHistory.swift
//  HealthPredictor
//
//  Created by Stephan  on 10.06.2025.
//

import Foundation

struct HealthMetricHistory: Codable {
    let daily: [Double]
    let monthly: [Double]
}
