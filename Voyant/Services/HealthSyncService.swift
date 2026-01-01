//
//  HealthSyncService.swift
//  Voyant
//

import Foundation
import HealthKit
#if canImport(UIKit)
import UIKit
#endif

final class HealthSyncService {

	static let shared = HealthSyncService()


	private let healthStoreService: HealthStoreService
	private let healthCSVExporter: HealthCSVExporter.Type
	private let agentBackendService: AgentBackendService
	private let authService: AuthService

	private init() {
		self.healthStoreService = HealthStoreService.shared
		self.healthCSVExporter = HealthCSVExporter.self
		self.agentBackendService = AgentBackendService.shared
		self.authService = AuthService.shared
	}

	private let healthStore = HKHealthStore()
	private let calendar = Calendar.current
	private var currentUserId: String?
	// How far back to recompute on delta uploads to catch late-arriving HealthKit sync/backfills.
	// Keep this bounded to avoid oversized uploads.
	private let lookbackHours: Int = 24

	// Flag to prevent observer uploads during initial seed
	private var isPerformingInitialSeed = false
	// Prevent duplicate initial seed scheduling (startBackgroundSync can be called multiple times on startup).
	private var initialSeedWorkScheduled = false

	// Debouncing for delta uploads to prevent multiple simultaneous uploads
	private var pendingDeltaWorkItem: DispatchWorkItem?
	private let deltaDebounceInterval: TimeInterval = 5.0 // Increased to 5 seconds for better coalescing
	private let deltaUploadQueue = DispatchQueue(label: "com.voyant.health.delta", qos: .background)

	// MARK: - First-time seed persistence
	private func initialSeedDoneKey(for userId: String) -> String {
		return "health_initial_seed_done_\(userId)"
	}

	// MARK: - Background Observers Setup
	// Call once after HealthKit authorization, independent of sign-in
	func enableBackgroundObservers() {
		// Check if already enabled
		if UserDefaults.standard.bool(forKey: "hk_background_observers_enabled") {
			print("[HealthSync] Background observers already enabled")
			return
		}

		print("[HealthSync] Enabling background observers for automatic sync")
		registerObservers()
		enableBackgroundDelivery()

		// Mark as enabled so we don't re-register
		UserDefaults.standard.set(true, forKey: "hk_background_observers_enabled")
		print("[HealthSync] Background observers enabled successfully")
	}

	// Call on sign-in to handle initial seed upload (one-time per user)
	func startBackgroundSync(userId: String) {
		// Record timezone at sync time so we capture travel changes even if the app wasn't foregrounded.
		// (TimezoneHistoryService dedupes if nothing changed.)
		TimezoneHistoryService.shared.recordCurrentTimeZone()
		GeoTimezoneHistoryService.shared.recordNow()

		// Store current user ID
		self.currentUserId = userId
		print("[HealthSync] startBackgroundSync for user=\(userId)")

		// Avoid duplicate seed scheduling during the same app session.
		if isPerformingInitialSeed || initialSeedWorkScheduled {
			print("[HealthSync] Initial seed already in progress/scheduled, skipping duplicate start")
			return
		}

		// Check if initial seed already done for this user
		let seedKey = self.initialSeedDoneKey(for: userId)
		if UserDefaults.standard.bool(forKey: seedKey) {
			print("[HealthSync] Initial seed already completed for user=\(userId), skipping")
			return
		}

		// Set flag to prevent observer uploads during initial seed
		self.isPerformingInitialSeed = true
		self.initialSeedWorkScheduled = true

		print("[HealthSync] Performing initial seed for user=\(userId)")

		// Delay initial seed to let any ongoing operations settle
		let initialSeed = DispatchWorkItem { [weak self] in
			guard let self = self else { return }

			// Initial full backfill (~164 days) to guarantee seed on first run
			HealthCSVExporter.generateCSV(for: userId, metrics: []) { res in
				switch res {
				case .success(let data):
						// Write debug CSV to Documents so it can be inspected in Files
						do {
							let ts = ISO8601DateFormatter().string(from: Date()).replacingOccurrences(of: ":", with: "-")
							if let docs = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask).first {
								let url = docs.appendingPathComponent("HealthExport-\(ts).csv")
								try data.write(to: url, options: .atomic)
								print("[HealthSync] Saved initial CSV to \(url.path)")
							}
						} catch {
							print("[HealthSync] Failed saving initial CSV: \(error)")
						}
						Task {
							do {
								print("[HealthSync] Uploading initial CSV (\(data.count) bytes)")
								let taskId = try await AgentBackendService.shared.uploadHealthCSV(data, uploadMode: "seed")
								print("[HealthSync] Enqueued process_csv_upload taskId=\(taskId)")
								// Seed is "done" from the client perspective once the backend accepted/enqueued it.
								// The worker can take >2 minutes on a cold DB; don't treat that as failure.
								await MainActor.run {
									self.isPerformingInitialSeed = false
									self.initialSeedWorkScheduled = false
									UserDefaults.standard.set(true, forKey: seedKey)
								}

								// Poll for completion for debug only (non-fatal if it takes too long).
								Task.detached(priority: .background) {
									do {
										let status = try await AgentBackendService.shared.waitForHealthTask(taskId, timeout: 600, pollInterval: 3)
										print("[HealthSync] Initial seed backend state=\(status.state)")
									} catch {
										print("[HealthSync] Initial seed status poll failed (non-fatal): \(error)")
									}
								}
							} catch {
								print("[HealthSync] Initial upload failed: \(error)")
								// Clear flags so user can retry later
								await MainActor.run {
									self.isPerformingInitialSeed = false
									self.initialSeedWorkScheduled = false
								}
							}
						}
				case .failure(let e):
					print("[HealthSync] generateCSV failed: \(e.localizedDescription)")
					// Clear flag even on failure
					self.isPerformingInitialSeed = false
					self.initialSeedWorkScheduled = false
				}
			}
		}
		// Execute the initial seed work item after delay
		DispatchQueue.main.asyncAfter(deadline: .now() + 3.0, execute: initialSeed)
	}

	private func registerObservers() {
		let quantityTypes: [HKQuantityTypeIdentifier] = [
			.heartRate, .restingHeartRate, .walkingHeartRateAverage, .heartRateVariabilitySDNN,
			.stepCount, .walkingSpeed, .vo2Max, .activeEnergyBurned, .dietaryWater,
			.bodyMass, .bodyMassIndex, .bloodGlucose, .oxygenSaturation,
			.bloodPressureSystolic, .bloodPressureDiastolic, .respiratoryRate, .bodyTemperature,
			.appleExerciseTime
		]
		let categoryTypes: [HKCategoryTypeIdentifier] = [.sleepAnalysis, .mindfulSession]

		let sampleTypes: [HKSampleType] =
			quantityTypes.compactMap { HKObjectType.quantityType(forIdentifier: $0) as HKSampleType? } +
			categoryTypes.compactMap { HKObjectType.categoryType(forIdentifier: $0) as HKSampleType? } +
			[HKObjectType.workoutType()]

		for t in sampleTypes {
			let query = HKObserverQuery(sampleType: t, predicate: nil) { [weak self] _, completionHandler, _ in
				self?.handleObserverEvent(completionHandler: completionHandler)
			}
			healthStore.execute(query)
		}
	}

	private func enableBackgroundDelivery() {
		let allTypes: [HKSampleType] = [
			HKObjectType.workoutType()
		] + [
			HKQuantityTypeIdentifier.heartRate,
			.restingHeartRate,
			.walkingHeartRateAverage,
			.heartRateVariabilitySDNN,
			.stepCount,
			.walkingSpeed,
			.vo2Max,
			.activeEnergyBurned,
			.dietaryWater,
			.bodyMass,
			.bodyMassIndex,
			.bloodGlucose,
			.oxygenSaturation,
			.bloodPressureSystolic,
			.bloodPressureDiastolic,
			.respiratoryRate,
			.bodyTemperature,
			.appleExerciseTime
		].compactMap { HKObjectType.quantityType(forIdentifier: $0) } + [
			HKObjectType.categoryType(forIdentifier: .sleepAnalysis),
			HKObjectType.categoryType(forIdentifier: .mindfulSession)
		].compactMap { $0 }

		for t in allTypes {
			healthStore.enableBackgroundDelivery(for: t, frequency: .immediate) { _, _ in }
		}
	}

	private func handleObserverEvent(completionHandler: @escaping () -> Void) {
		// Immediately complete the HealthKit callback to keep the system happy
		completionHandler()

		// Capture timezone whenever we get woken up for HealthKit background delivery.
		TimezoneHistoryService.shared.recordCurrentTimeZone()
		GeoTimezoneHistoryService.shared.recordNow()

		// Skip if we're doing initial seed
		if isPerformingInitialSeed {
			print("[HealthSync] Skipping observer event during initial seed")
			return
		}

		// Process delta uploads whether app is foreground or background
		// iOS will launch the app in background when health data changes

		// Cancel any pending delta upload
		pendingDeltaWorkItem?.cancel()

		// Create a new debounced work item
		let workItem = DispatchWorkItem { [weak self] in
			self?.performDeltaUpload()
		}
		pendingDeltaWorkItem = workItem

		// Schedule the delta upload after debounce interval
		deltaUploadQueue.asyncAfter(deadline: .now() + deltaDebounceInterval, execute: workItem)
	}

	private func performDeltaUpload() {
		// Use anchored queries for tighter deltas; fallback to small lookback if no changes detected
		let now = Date()
		let group = DispatchGroup()
		var minChangedDate: Date?

		let sampleTypes: [HKSampleType] = {
			let q: [HKSampleType] = [
				HKQuantityTypeIdentifier.heartRate,
				.restingHeartRate,
				.walkingHeartRateAverage,
				.heartRateVariabilitySDNN,
				.stepCount,
				.walkingSpeed,
				.vo2Max,
				.activeEnergyBurned,
				.dietaryWater,
				.bodyMass,
				.bodyMassIndex,
				.bloodGlucose,
				.oxygenSaturation,
				.bloodPressureSystolic,
				.bloodPressureDiastolic,
				.respiratoryRate,
				.bodyTemperature,
				.appleExerciseTime
			].compactMap { HKObjectType.quantityType(forIdentifier: $0) as HKSampleType? }
			let c: [HKSampleType] = [
				HKObjectType.categoryType(forIdentifier: .sleepAnalysis),
				HKObjectType.categoryType(forIdentifier: .mindfulSession)
			].compactMap { $0 }
			let w: [HKSampleType] = [HKObjectType.workoutType()]
			return q + c + w
		}()

		// If we don't have anchors yet (first run), anchored queries can return *all history*,
		// which can create massive "delta" windows and huge CSV uploads. In that case:
		// - We still run anchored queries to establish anchors.
		// - But we do NOT widen the delta window based on returned historical samples.
		var anyMissingAnchor = false
		for type in sampleTypes {
			if loadAnchor(for: type) == nil {
				anyMissingAnchor = true
				break
			}
		}

		for type in sampleTypes {
			group.enter()
			let anchor = loadAnchor(for: type)
			let anchored = HKAnchoredObjectQuery(type: type, predicate: nil, anchor: anchor, limit: HKObjectQueryNoLimit) { [weak self] _, samplesOrNil, deleted, newAnchor, error in
				defer { group.leave() }
				guard let self = self else { return }
				if let newAnchor = newAnchor {
					self.saveAnchor(newAnchor, for: type)
				}
				if error != nil { return }

				// If this is the first run (missing anchors), don't treat historical samples as "delta".
				// We'll keep the delta window bounded below after the group completes.
				if anyMissingAnchor {
					return
				}
				let samples = samplesOrNil ?? []
				// Consider both additions and deletions; deletions don't give startDate, so we keep small lookback anyway
				for s in samples {
					let start = (s as? HKWorkout)?.startDate ?? s.startDate
					if let currentMin = minChangedDate {
						if start < currentMin { minChangedDate = start }
					} else {
						minChangedDate = start
					}
				}
                if let deleted = deleted, !deleted.isEmpty {
					// Force a lookback by setting minChangedDate if none yet
					if minChangedDate == nil {
						minChangedDate = now.addingTimeInterval(-Double(lookbackHours) * 3600.0)
					}
				}
			}
			healthStore.execute(anchored)
		}

		group.notify(queue: .main) { [weak self] in
			guard let self = self else { return }
			let end = now
			let start: Date = {
				// If anchors were missing, keep a small delta window so uploads stay small.
				if anyMissingAnchor {
					return now.addingTimeInterval(-Double(self.lookbackHours) * 3600.0)
				}
				if let minChangedDate = minChangedDate {
					return min(minChangedDate, now.addingTimeInterval(-Double(self.lookbackHours) * 3600.0))
				}
				return now.addingTimeInterval(-Double(self.lookbackHours) * 3600.0)
			}()

			Task {
				var userId = self.currentUserId
				if userId == nil {
					userId = try? await AuthService.getUserId()
					self.currentUserId = userId
				}
				guard let uid = userId else { return }

				let minuteResolution = end.timeIntervalSince(start) <= (90 * 60)

				print("[HealthSync] Creating delta CSV for period: \(start) to \(end)")

				HealthCSVExporter.generateDeltaCSV(for: uid, start: start, end: end, metrics: [], minuteResolution: minuteResolution) { result in
					switch result {
					case .success(let data):
						// Save delta CSV for debugging
						do {
							let ts = ISO8601DateFormatter().string(from: Date()).replacingOccurrences(of: ":", with: "-")
							if let docs = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask).first {
								let url = docs.appendingPathComponent("HealthDelta-\(ts).csv")
								try data.write(to: url, options: .atomic)
								print("[HealthSync] Saved delta CSV to \(url.path) (\(data.count) bytes)")
							}
						} catch {
							print("[HealthSync] Failed saving delta CSV: \(error)")
						}
						Task {
							do {
								print("[HealthSync] Uploading delta CSV (\(data.count) bytes)")
								let taskId = try await AgentBackendService.shared.uploadHealthCSV(data, uploadMode: "delta")
								print("[HealthSync] Delta upload enqueued with task_id=\(taskId)")
							} catch {
								print("[HealthSync] Delta upload failed: \(error)")
							}
						}
					case .failure(let error):
						print("[HealthSync] Failed to generate delta CSV: \(error)")
					}
				}
			}
		}
	}

	// MARK: - Anchor persistence
	private func anchorKey(for type: HKSampleType) -> String {
		if let qt = type as? HKQuantityType {
			return "hk_anchor_quantity_\(qt.identifier)"
		}
		if let ct = type as? HKCategoryType {
			return "hk_anchor_category_\(ct.identifier)"
		}
		if type is HKWorkoutType {
			return "hk_anchor_workout"
		}
		return "hk_anchor_unknown"
	}

	private func loadAnchor(for type: HKSampleType) -> HKQueryAnchor? {
		let key = anchorKey(for: type)
		guard let data = UserDefaults.standard.data(forKey: key) else { return nil }
		do {
			let anchor = try NSKeyedUnarchiver.unarchivedObject(ofClass: HKQueryAnchor.self, from: data)
			return anchor
		} catch {
			return nil
		}
	}

	private func saveAnchor(_ anchor: HKQueryAnchor, for type: HKSampleType) {
		let key = anchorKey(for: type)
		do {
			let data = try NSKeyedArchiver.archivedData(withRootObject: anchor, requiringSecureCoding: true)
			UserDefaults.standard.set(data, forKey: key)
		} catch {
			// ignore persistence errors
		}
	}
}


