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
    private var backgroundUpdatesStarted = false

    struct Place: Equatable {
        let country: String?
        let region: String?
        let city: String?
    }

    private override init() {
        super.init()
        manager.delegate = self
        // We want enough resolution to get at least city.
        manager.desiredAccuracy = kCLLocationAccuracyKilometer
        manager.distanceFilter = 500 // meters; coarse to reduce battery
        manager.activityType = .fitness
#if canImport(UIKit)
        // Requires UIBackgroundModes = location.
        manager.allowsBackgroundLocationUpdates = true
#endif
        manager.pausesLocationUpdatesAutomatically = true
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

        // Request "Always" so we can capture location even when the user isn't actively using the app
        // (e.g., while Apple Watch workouts/metrics are being written and HealthKit wakes the app).
        let status = manager.authorizationStatus
        switch status {
        case .notDetermined:
            #if canImport(UIKit)
            manager.requestAlwaysAuthorization()
            #endif
        default:
            if isAuthorized(status) {
                if isAuthorizedAlways(status) {
                    startBackgroundLocationUpdatesIfNeeded()
                } else {
                    // Attempt to upgrade to Always (iOS may require a user journey / Settings depending on OS version).
                    #if canImport(UIKit)
                    manager.requestAlwaysAuthorization()
                    #endif
                }
                manager.requestLocation()
            }
        }
    }

    /// Best-effort: request a location now and record its timezone.
    /// Safe to call frequently; persistence dedupes consecutive identical tz.
    func recordNow() {
        let status = manager.authorizationStatus
        guard isAuthorized(status) else { return }
        if isAuthorizedAlways(status) {
            startBackgroundLocationUpdatesIfNeeded()
            manager.requestLocation()
            return
        }
        // Fallback: if only WhenInUse is granted, we can only snapshot while the app is active.
        #if canImport(UIKit)
        if UIApplication.shared.applicationState == .active {
            manager.requestLocation()
        }
        #else
        manager.requestLocation()
        #endif
    }

    // MARK: - Queries

    /// Returns the most recent *geocoded* entry (source == "geocode") effective at or before `date`.
    /// If the only entry is the seed_device_tz placeholder, returns nil so callers can fall back to other sources.
    func geocodedEntryIfKnown(for date: Date) -> (tzName: String, place: Place?)? {
        return queue.sync {
            let entries = loadEntries()
            guard !entries.isEmpty else { return nil }
            if date < entries[0].effectiveAt { return nil }

            var candidate: Entry?
            for e in entries {
                if e.effectiveAt <= date {
                    candidate = e
                } else {
                    break
                }
            }
            guard let c = candidate, c.source == "geocode" else { return nil }
            return (c.tzName, c.place)
        }
    }

    // MARK: - CLLocationManagerDelegate

    func locationManagerDidChangeAuthorization(_ manager: CLLocationManager) {
        let status = manager.authorizationStatus
        if isAuthorized(status) {
            if isAuthorizedAlways(status) {
                startBackgroundLocationUpdatesIfNeeded()
            } else {
                // Attempt to upgrade to Always once we have at least some authorization.
                #if canImport(UIKit)
                manager.requestAlwaysAuthorization()
                #endif
            }
            manager.requestLocation()
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
            let pm = placemarks?.first
            let tzName = pm?.timeZone?.identifier
            let name = tzName?.isEmpty == false ? tzName! : TimeZone.current.identifier
            let place = Place(
                country: pm?.country,
                region: pm?.administrativeArea,
                city: pm?.locality
            )
            self.queue.sync {
                self.appendEntry(effectiveAt: Date(), tzName: name, source: "geocode", place: place)
            }
        }
        #else
        _ = loc
        #endif
    }

    func locationManager(_ manager: CLLocationManager, didFailWithError error: Error) {
        // Best-effort only; ignore.
    }

    private func startBackgroundLocationUpdatesIfNeeded() {
        guard !backgroundUpdatesStarted else { return }
        backgroundUpdatesStarted = true
        // "Significant changes" is the lowest-battery way to get background location.
        // It will wake the app periodically as the user moves (useful for workouts/travel).
        manager.startMonitoringSignificantLocationChanges()
    }

    // MARK: - Authorization helpers
    // Avoid referencing iOS-only enum cases (e.g., .authorizedWhenInUse) because some toolchains index this file under macOS.
    private func isAuthorized(_ status: CLAuthorizationStatus) -> Bool {
        status.rawValue >= CLAuthorizationStatus.authorizedAlways.rawValue
    }

    private func isAuthorizedAlways(_ status: CLAuthorizationStatus) -> Bool {
        status.rawValue == CLAuthorizationStatus.authorizedAlways.rawValue
    }

    // MARK: - Persistence

    private struct Entry {
        let effectiveAt: Date
        let tzName: String
        let source: String
        let place: Place?
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
            let place = Place(
                country: d["place_country"] as? String,
                region: d["place_region"] as? String,
                city: d["place_city"] as? String
            )
            let hasPlace = (place.country?.isEmpty == false) || (place.region?.isEmpty == false) || (place.city?.isEmpty == false)
            return Entry(
                effectiveAt: Date(timeIntervalSince1970: ts),
                tzName: tz,
                source: src,
                place: hasPlace ? place : nil
            )
        }
        return entries.sorted(by: { $0.effectiveAt < $1.effectiveAt })
    }

    private func appendEntry(effectiveAt: Date, tzName: String, source: String, place: Place? = nil) {
        let tz = tzName.isEmpty ? TimeZone.current.identifier : tzName
        var raw = (UserDefaults.standard.array(forKey: storageKey) as? [[String: Any]]) ?? []

        // Avoid duplicates
        if let last = raw.last, let lastTz = last["tz"] as? String, lastTz == tz {
            return
        }

        var row: [String: Any] = [
            "ts": effectiveAt.timeIntervalSince1970,
            "tz": tz,
            "src": source,
        ]
        if let p = place {
            if let v = p.country, !v.isEmpty { row["place_country"] = v }
            if let v = p.region, !v.isEmpty { row["place_region"] = v }
            if let v = p.city, !v.isEmpty { row["place_city"] = v }
        }
        raw.append(row)

        // Keep bounded
        if raw.count > 64 {
            raw = Array(raw.suffix(64))
        }
        UserDefaults.standard.set(raw, forKey: storageKey)
    }
}


