//
//  UserFileCacheService.swift
//  HealthPredictor
//
//  Created by Assistant on 2025.
//

import Foundation
import HealthKit

// MARK: - Error Types

enum HealthFileCacheError: Error {
    case fileNotFound
    case fileCreationFailed
    case uploadFailed
}

class UserFileCacheService: ObservableObject, UserFileCacheServiceProtocol {

    static let shared = UserFileCacheService()

    private let healthFileCreationService: UserFileCreationServiceProtocol
    private let fileUploadService: FileUploadToBackendServiceProtocol
    private let healthStoreService: HealthStoreServiceProtocol

    private init() {
        self.healthFileCreationService = UserFileCreationService.shared
        self.fileUploadService = FileUploadToBackendService.shared
        self.healthStoreService = HealthStoreService.shared
    }

    private let cacheFileURLKey = "cached_health_file_url"
    private let cacheTimestampKey = "cached_health_file_timestamp"
    private let cacheS3URLKey = "cached_health_file_s3_url"

    private let cacheDuration: TimeInterval = 15 * 60  // Cache is valid for 15 minutes

    @Published var isUpdatingCache = false

    private var backgroundTimer: Timer?
    private var healthObservers: [HKObserverQuery] = []

    // Get cached file path or create new one if needed
    func getCachedHealthFile() async throws -> String {
        if let cachedPath = UserDefaults.standard.string(forKey: cacheFileURLKey), isCacheValid() {
            // Check if the cached file actually exists
            if FileManager.default.fileExists(atPath: cachedPath) {
                print("Using cached health file: \(cachedPath)")
                return cachedPath
            } else {
                print("Cached file doesn't exist, creating new one: \(cachedPath)")
                // Clear the invalid cache
                UserDefaults.standard.removeObject(forKey: cacheFileURLKey)
                UserDefaults.standard.removeObject(forKey: cacheTimestampKey)
            }
        }

        return try await createAndCacheHealthFile()
    }

    // Setup CSV file generation with observers and timers
    func setupCSVFile() {
        performCleanup()
        setupCriticalObservers()
        startBackgroundTimer()

        print("Cleanup, critical observers, and background timer setup complete")
    }

    // Clean up all observers and timers
    func performCleanup() {
        // Stop and clear existing observers
        healthObservers.forEach { observer in
            healthStoreService.healthStore.stop(observer)
        }
        healthObservers.removeAll()

        // Stop and clear existing timer
        backgroundTimer?.invalidate()
        backgroundTimer = nil

        print("Observers/timer clean up complete")
    }

    // Setup real-time observers for critical metrics
    private func setupCriticalObservers() {
        let criticalMetrics: [String: HKQuantityTypeIdentifier] = [
            "Resting HR": .restingHeartRate,
            "Walking HR": .walkingHeartRateAverage,
            "Blood Glucose": .bloodGlucose
        ]

        for (metricName, typeIdentifier) in criticalMetrics {
            guard let quantityType = HKQuantityType.quantityType(forIdentifier: typeIdentifier) else {
                print("Failed to create quantity type for \(metricName)")
                continue
            }

            // Permission for enabling background delivery
            healthStoreService.healthStore.enableBackgroundDelivery(for: quantityType, frequency: .immediate) { [weak self] success, error in
                if success {
                    print("Background delivery enabled for metric: \(metricName)")

                    // After permission is granted, set up observer that listens to changes
                    let observerQuery = HKObserverQuery(sampleType: quantityType, predicate: nil) { [weak self] query, completion, error in
                        if let error = error {
                            print("Observer error for \(metricName): \(error.localizedDescription)")
                            completion()
                            return
                        }

                        // Check if there's actually data before triggering update
                        Task {
                            let hasData = await self?.checkIfMetricHasData(quantityType: quantityType) ?? false
                            if hasData {
                                print("\(metricName) has chanegd, updating health file")
                                do {
                                    _ = try await self?.getCachedHealthFile()
                                } catch {
                                    print("Failed to update health file: \(error)")
                                }
                            } else {
                                print("Metric \(metricName) observer fired but no data available")
                            }
                        }
                        completion()
                    }

                    self?.healthObservers.append(observerQuery)
                    self?.healthStoreService.healthStore.execute(observerQuery)
                } else {
                    print("Failed to enable background delivery for \(metricName): \(error?.localizedDescription ?? "Unknown error")")
                    print("Skipping observer setup for \(metricName)")
                }
            }
        }
    }

    // Start background timer for slow-changing metrics (15-minute intervals)
    private func startBackgroundTimer() {
        backgroundTimer = Timer.scheduledTimer(withTimeInterval: 15 * 60, repeats: true) { _ in
            Task {
                do {
                    _ = try await self.getCachedHealthFile()
                } catch {
                    print("Background refresh failed: \(error)")
                }
            }
        }
        print("Background timer started for all other metrics")
    }

    // Create and cache health data file, then upload to cloud
    private func createAndCacheHealthFile() async throws -> String {
        isUpdatingCache = true
        defer { isUpdatingCache = false }

        let filePath = try await generateCSVAsync()

        UserDefaults.standard.set(filePath, forKey: cacheFileURLKey)
        UserDefaults.standard.set(Date().timeIntervalSince1970, forKey: cacheTimestampKey)

        await uploadToTigris(filePath: filePath)

        print("Health file cache complete: \(filePath)")
        return filePath
    }

    // Generate CSV file asynchronously
    private func generateCSVAsync() async throws -> String {
        return try await withCheckedThrowingContinuation { continuation in
            healthFileCreationService.generateCSV { url in
                if let url = url {
                    continuation.resume(returning: url.path)
                } else {
                    continuation.resume(throwing: HealthFileCacheError.fileNotFound)
                }
            }
        }
    }

    // Upload file to Tigris cloud storage
    private func uploadToTigris(filePath: String) async {
        do {
            let fileData = try FileUtilities.readFileData(from: filePath)

            let s3URL = try await fileUploadService.uploadHealthDataFile(fileData: fileData)

            UserDefaults.standard.set(s3URL, forKey: cacheS3URLKey)
            print("Successfully uploaded to Tigris: \(s3URL)")
        } catch {
            print("Failed to upload to Tigris: \(error)")  // Don't throw - we can still use local file
        }
    }

    /// Check if cached file is still valid (less than 15 minutes old)
    private func isCacheValid() -> Bool {
        guard let timestamp = UserDefaults.standard.object(forKey: cacheTimestampKey) as? TimeInterval else {
            return false
        }

        let cacheAge = Date().timeIntervalSince1970 - timestamp
        return cacheAge < cacheDuration
    }

    /// Check if a metric actually has data available
    private func checkIfMetricHasData(quantityType: HKQuantityType) async -> Bool {
        let now = Date()
        let startDate = Calendar.current.date(byAdding: .day, value: -1, to: now) ?? now
        let predicate = HKQuery.predicateForSamples(withStart: startDate, end: now, options: .strictStartDate)

        return await withCheckedContinuation { continuation in
            let query = HKSampleQuery(sampleType: quantityType, predicate: predicate, limit: 1, sortDescriptors: nil) { _, samples, error in
                let hasData = samples?.isEmpty == false
                continuation.resume(returning: hasData)
            }
            healthStoreService.healthStore.execute(query)
        }
    }
}