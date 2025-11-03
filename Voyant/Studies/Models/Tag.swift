//
//  Tag.swift
//  HealthPredictor
//
//  Created by Stephan  on 27.05.2025.
//

import Foundation
import SwiftUI

struct Tag: Identifiable, Hashable {
    let id = UUID()
    let name: String
    let color: Color
    let subtags: [String]

    static let healthKeywords: [Tag] = [
        Tag(name: "Heart", color: .red, subtags: ["HRV", "Resting HR", "Walking HR", "Blood Pressure"]),
        Tag(name: "Activity", color: .green, subtags: ["Step Count", "Walking Speed", "VO2 Max"]),
        Tag(name: "Sleep", color: .blue, subtags: ["Sleep Duration"]),
        Tag(name: "Calorie", color: .yellow, subtags: ["Active Energy"]),
        Tag(name: "Water", color: .cyan, subtags: ["Hydration"]),
        Tag(name: "Mind", color: .purple, subtags: ["Mindfulness Minutes"]),
        Tag(name: "Weight", color: .pink, subtags: ["Weight", "BMI"]),
        Tag(name: "Glucose", color: .orange, subtags: ["Blood Glucose"]),
        Tag(name: "Oxygen", color: .teal, subtags: ["Oxygen Saturation"])
    ]
}
