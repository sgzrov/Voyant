import Foundation

/// Tracks device timezone changes over time so we can present historical events in the timezone
/// that was active when the event occurred (e.g., LA workout stays LA time after traveling).
final class TimezoneHistoryService {
    static let shared = TimezoneHistoryService()

    private let storageKey = "voyant_timezone_history_v1"
    private let queue = DispatchQueue(label: "Voyant.TimezoneHistoryService")

    private init() {}

    func start() {
        queue.sync {
            // Seed with current tz if empty
            if loadEntries().isEmpty {
                appendEntry(effectiveAt: Date(), tzName: TimeZone.current.identifier)
            }
        }
        NotificationCenter.default.addObserver(
            self,
            selector: #selector(handleTimeZoneDidChange),
            name: .NSSystemTimeZoneDidChange,
            object: nil
        )
    }

    @objc private func handleTimeZoneDidChange() {
        recordCurrentTimeZone()
    }

    func recordCurrentTimeZone() {
        queue.sync {
            appendEntry(effectiveAt: Date(), tzName: TimeZone.current.identifier)
        }
    }

    /// Returns a timezone identifier only if the service has a recorded entry that is effective at or before `date`.
    /// If `date` is earlier than our first recorded entry, returns nil (unknown).
    func tzNameIfKnown(for date: Date) -> String? {
        return queue.sync {
            let entries = loadEntries()
            guard !entries.isEmpty else { return nil }
            // If the queried date predates our first recorded entry, we don't actually know.
            if date < entries[0].effectiveAt { return nil }

            var candidate = entries[0].tzName
            for e in entries {
                if e.effectiveAt <= date {
                    candidate = e.tzName
                } else {
                    break
                }
            }
            return candidate
        }
    }

    /// Returns UTC offset minutes if the timezone is known for that date (see `tzNameIfKnown`), else nil.
    func utcOffsetMinutesIfKnown(for date: Date) -> Int? {
        guard let name = tzNameIfKnown(for: date), let tz = TimeZone(identifier: name) else { return nil }
        return tz.secondsFromGMT(for: date) / 60
    }

    // MARK: - Persistence

    private struct Entry {
        let effectiveAt: Date
        let tzName: String
    }

    private func loadEntries() -> [Entry] {
        guard let raw = UserDefaults.standard.array(forKey: storageKey) as? [[String: Any]] else {
            return []
        }
        let entries: [Entry] = raw.compactMap { d in
            guard
                let ts = d["ts"] as? Double,
                let tz = d["tz"] as? String,
                !tz.isEmpty
            else { return nil }
            return Entry(effectiveAt: Date(timeIntervalSince1970: ts), tzName: tz)
        }
        return entries.sorted(by: { $0.effectiveAt < $1.effectiveAt })
    }

    private func appendEntry(effectiveAt: Date, tzName: String) {
        let tz = tzName.isEmpty ? TimeZone.current.identifier : tzName
        var raw = (UserDefaults.standard.array(forKey: storageKey) as? [[String: Any]]) ?? []

        // Avoid spamming duplicates if nothing changed
        if let last = raw.last, let lastTz = last["tz"] as? String, lastTz == tz {
            return
        }

        raw.append([
            "ts": effectiveAt.timeIntervalSince1970,
            "tz": tz,
        ])
        // Keep it bounded (we only need a small history)
        if raw.count > 64 {
            raw = Array(raw.suffix(64))
        }
        UserDefaults.standard.set(raw, forKey: storageKey)
    }
}


