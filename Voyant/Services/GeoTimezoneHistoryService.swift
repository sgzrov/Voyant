import Foundation
import CoreLocation
#if canImport(UIKit)
import UIKit
#endif

/// Records a sparse history of the user's timezone derived from geocoding their location.
///
/// Why this exists:
/// - HealthKit workout timezone metadata is optional and not reliably present.
/// - An absolute timestamp alone cannot tell you "where you were".
/// - If we store (effectiveAt -> tz_name) entries derived from coordinates, we can later
///   stamp events/metrics with the timezone that was active where the user actually was.
///
/// Notes:
/// - This service intentionally keeps a small bounded history.
/// - It can only be accurate from the moment we start recording. Earlier timestamps are "unknown".
final class GeoTimezoneHistoryService: NSObject, CLLocationManagerDelegate {
    static let shared = GeoTimezoneHistoryService()

    private let storageKey = "voyant_geo_timezone_history_v1"
    private let queue = DispatchQueue(label: "Voyant.GeoTimezoneHistoryService")

    private let manager = CLLocationManager()

    // Callbacks to run once the user has responded to the location permission prompt
    // (i.e., authorization is no longer .notDetermined), or after a timeout.
    private var authDeterminedCallbacks: [() -> Void] = []
    private var authTimeoutWorkItem: DispatchWorkItem?

    private override init() {
        super.init()
        manager.delegate = self
        manager.desiredAccuracy = kCLLocationAccuracyThreeKilometers
    }

    /// Start recording. This may trigger a location permission prompt depending on current status.
    func start() {
        queue.sync {
            // Seed a best-effort entry using the device timezone if empty so we have *something*
            // (useful for "now-ish" events even if location isn't granted).
            if loadEntries().isEmpty {
                appendEntry(effectiveAt: Date(), tzName: TimeZone.current.identifier, source: "seed_device_tz")
            }
        }

        // Ask for permission if needed; otherwise request a single location to geocode.
        let status = manager.authorizationStatus
        switch status {
        case .notDetermined:
            manager.requestWhenInUseAuthorization()
        default:
            // Avoid platform-specific enum cases (some toolchains/lints evaluate this file under macOS).
            // Treat any "authorized" state as allowed.
            if status.rawValue >= CLAuthorizationStatus.authorizedAlways.rawValue {
                manager.requestLocation()
            }
            break
        }
    }

    /// Ensures the user has responded to the location authorization prompt before calling `completion`.
    /// If authorization is already determined, calls `completion` immediately.
    /// If not determined, triggers the prompt (via `start()`) and waits until the user responds.
    ///
    /// - timeoutSeconds: Optional safety timeout. If nil, waits indefinitely (strict ordering).
    func whenAuthorizationDetermined(timeoutSeconds: TimeInterval? = 6.0, completion: @escaping () -> Void) {
        let status = manager.authorizationStatus
        if status != .notDetermined {
            completion()
            return
        }

        queue.async {
            self.authDeterminedCallbacks.append(completion)
        }

        // Ensure the prompt is shown.
        start()

        // Optional safety timeout so we don't block forever if user ignores the prompt.
        if let timeoutSeconds = timeoutSeconds, timeoutSeconds > 0 {
            let wi = DispatchWorkItem { [weak self] in
                self?.flushAuthDeterminedCallbacks()
            }
            authTimeoutWorkItem?.cancel()
            authTimeoutWorkItem = wi
            DispatchQueue.main.asyncAfter(deadline: .now() + timeoutSeconds, execute: wi)
        }
    }

    /// Best-effort: request a location now and record its timezone.
    /// Safe to call frequently; persistence dedupes consecutive identical tz.
    func recordNow() {
        let status = manager.authorizationStatus
        // Treat any "authorized" state as allowed (covers iOS WhenInUse + Always, and macOS Authorized).
        guard status.rawValue >= CLAuthorizationStatus.authorizedAlways.rawValue else { return }
        manager.requestLocation()
    }

    // MARK: - Queries

    /// Returns a timezone identifier only if we have a recorded entry effective at or before `date`.
    /// If `date` is earlier than our first recorded entry, returns nil (unknown).
    func tzNameIfKnown(for date: Date) -> String? {
        return queue.sync {
            let entries = loadEntries()
            guard !entries.isEmpty else { return nil }
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

    func utcOffsetMinutesIfKnown(for date: Date) -> Int? {
        guard let name = tzNameIfKnown(for: date), let tz = TimeZone(identifier: name) else { return nil }
        return tz.secondsFromGMT(for: date) / 60
    }

    // MARK: - CLLocationManagerDelegate

    func locationManagerDidChangeAuthorization(_ manager: CLLocationManager) {
        let status = manager.authorizationStatus
        if status.rawValue >= CLAuthorizationStatus.authorizedAlways.rawValue {
            manager.requestLocation()
        }
        if status != .notDetermined {
            flushAuthDeterminedCallbacks()
        }
    }

    func locationManager(_ manager: CLLocationManager, didUpdateLocations locations: [CLLocation]) {
        guard let loc = locations.last else { return }
        // Reverse geocode to timezone.
        //
        // Note: Some toolchains show CLGeocoder as deprecated for macOS 26. This project is iOS-only;
        // we compile the reverse-geocode path only for UIKit platforms to avoid macOS-only deprecation
        // noise in the editor/indexer while keeping the traditional iOS implementation.
        #if canImport(UIKit)
        CLGeocoder().reverseGeocodeLocation(loc) { [weak self] placemarks, _ in
            guard let self = self else { return }
            let tzName = placemarks?.first?.timeZone?.identifier
            let name = tzName?.isEmpty == false ? tzName! : TimeZone.current.identifier
            self.queue.sync {
                self.appendEntry(effectiveAt: Date(), tzName: name, source: "geocode")
            }
        }
        #else
        _ = loc
        #endif
    }

    func locationManager(_ manager: CLLocationManager, didFailWithError error: Error) {
        // Best-effort only; ignore.
    }

    private func flushAuthDeterminedCallbacks() {
        authTimeoutWorkItem?.cancel()
        authTimeoutWorkItem = nil
        let callbacks: [() -> Void] = queue.sync {
            let cbs = self.authDeterminedCallbacks
            self.authDeterminedCallbacks.removeAll()
            return cbs
        }
        guard !callbacks.isEmpty else { return }
        DispatchQueue.main.async {
            callbacks.forEach { $0() }
        }
    }

    // MARK: - Persistence

    private struct Entry {
        let effectiveAt: Date
        let tzName: String
        let source: String
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
            let src = (d["src"] as? String) ?? "unknown"
            return Entry(effectiveAt: Date(timeIntervalSince1970: ts), tzName: tz, source: src)
        }
        return entries.sorted(by: { $0.effectiveAt < $1.effectiveAt })
    }

    private func appendEntry(effectiveAt: Date, tzName: String, source: String) {
        let tz = tzName.isEmpty ? TimeZone.current.identifier : tzName
        var raw = (UserDefaults.standard.array(forKey: storageKey) as? [[String: Any]]) ?? []

        // Avoid duplicates
        if let last = raw.last, let lastTz = last["tz"] as? String, lastTz == tz {
            return
        }

        raw.append([
            "ts": effectiveAt.timeIntervalSince1970,
            "tz": tz,
            "src": source,
        ])

        // Keep bounded
        if raw.count > 64 {
            raw = Array(raw.suffix(64))
        }
        UserDefaults.standard.set(raw, forKey: storageKey)
    }
}


