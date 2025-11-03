//
//  Study.swift
//  HealthPredictor
//
//  Created by Stephan  on 05.06.2025.
//

import Foundation
import SwiftUI

struct Study: Identifiable, Hashable, Codable {
    let id: UUID
    var studyId: String?
    let title: String
    var summary: String
    var outcome: String
    let importDate: Date?

    init(id: UUID = UUID(), studyId: String? = nil, title: String, summary: String, outcome: String, importDate: Date? = nil) {
        self.id = id
        self.studyId = studyId
        self.title = title
        self.summary = summary
        self.outcome = outcome
        self.importDate = importDate
    }

    enum CodingKeys: String, CodingKey {
        case id, studyId = "study_id", title, summary, outcome
        case importDate = "import_date"
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)

        // Convert the Int ID sent from the backend into a UUID
        let intId = try container.decode(Int.self, forKey: .id)
        let uuidString = "\(String(format: "%08d", intId))-0000-0000-0000-000000000000"
        guard let uuid = UUID(uuidString: uuidString) else {
            throw DecodingError.dataCorruptedError(forKey: .id, in: container, debugDescription: "Failed to convert integer ID \(intId) to UUID")
        }
        print("[STUDY_ID] Converted backend ID \(intId) to UUID: \(uuid)")

        self.id = uuid
        self.studyId = try container.decode(String.self, forKey: .studyId)
        self.title = try container.decode(String.self, forKey: .title)
        self.summary = try container.decode(String.self, forKey: .summary)
        self.outcome = try container.decode(String.self, forKey: .outcome)
        self.importDate = try container.decodeIfPresent(Date.self, forKey: .importDate)
    }

    // - For existing studies: returns study_id from backend
    // - For new local studies: returns temporary UUID until backend assigns study_id
    var backendId: String {
        if let studyId = studyId {
            return studyId
        } else {
            let tempId = UUID().uuidString
            print("[STUDY_ID] Study has no studyId, backendId should only be created for new studies: \(tempId)")
            return tempId
        }
    }

    func hash(into hasher: inout Hasher) {
        hasher.combine(studyId)
    }

    static func == (lhs: Study, rhs: Study) -> Bool {
        lhs.studyId == rhs.studyId
    }
}
