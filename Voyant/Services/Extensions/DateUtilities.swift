//
//  DateUtilities.swift
//  HealthPredictor
//
//  Created by Stephan  on 22.06.2025.
//

import Foundation

struct DateUtilities {

    // Parse dates from backend API responses (ISO8601 with microseconds)
    static func parseBackendDate(_ dateString: String?) -> Date? {
        guard let dateString = dateString else { return nil }

        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(secondsFromGMT: 0)

        formatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSZZZZZ"
        let parsedDate = formatter.date(from: dateString)

        return parsedDate
    }

    // UI Formatting (e.g., "Jan 1st 2025 at 14:30")
    static func formatDisplayDate(_ date: Date) -> String {
        let formatter = DateFormatter()
        formatter.dateFormat = "MMM d yyyy 'at' HH:mm"

        let calendar = Calendar.current
        let day = calendar.component(.day, from: date)
        let suffix = ordinalSuffix(for: day)

        let dateString = formatter.string(from: date)

        // Replace only the first occurrence of the day number (which is the day, not the year)
        let dayString = String(day)
        if let firstRange = dateString.range(of: dayString) {
            return dateString.replacingCharacters(in: firstRange, with: dayString + suffix)
        }

        return dateString
    }

    // Get ordinal suffix for day number
    private static func ordinalSuffix(for day: Int) -> String {
        switch day {
        case 1, 21, 31: return "st"
        case 2, 22: return "nd"
        case 3, 23: return "rd"
        default: return "th"
        }
    }

    // MARK: - JSON Decoder Integration

    static func createBackendDecoder() -> JSONDecoder {
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .custom { decoder in
            let container = try decoder.singleValueContainer()
            let dateString = try container.decode(String.self)

            guard let date = parseBackendDate(dateString) else {
                throw DecodingError.dataCorruptedError(
                    in: container,
                    debugDescription: "Cannot decode date string \(dateString)"
                )
            }
            return date
        }
        return decoder
    }
}