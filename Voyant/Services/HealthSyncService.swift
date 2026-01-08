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

	private init() {
		self.healthStoreService = HealthStoreService.shared
	}

	private let healthStore = HKHealthStore()
	private let calendar = Calendar.current
	private var currentUserId: String?
	private var pendingSeedUserId: String?
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
	private func initialSeedBytesKey(for userId: String) -> String {
		return "health_initial_seed_bytes_\(userId)"
	}
	private func initialSeedAuthorizedKey(for userId: String) -> String {
		return "health_initial_seed_ran_after_hk_auth_\(userId)"
	}

	/// Called once HealthKit authorization is granted (from `HealthPredictorApp`).
	/// If a seed was deferred because auth wasn't ready yet, this will kick it off.
	func notifyHealthKitAuthorized() {
		guard healthStoreService.isReadGrantedCached() else {
			print("[HealthSync] HealthKit read not granted; seed will remain deferred")
			return
		}
		if let uid = pendingSeedUserId {
			pendingSeedUserId = nil
			startBackgroundSync(userId: uid)
		}
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
		// Record timezone at sync time so we capture travel changes even if geo isn't available.
		// (TimezoneHistoryService dedupes if nothing changed.)
		TimezoneHistoryService.shared.recordCurrentTimeZone()
		GeoTimezoneHistoryService.shared.recordNow()

		// Store current user ID
		self.currentUserId = userId
		print("[HealthSync] startBackgroundSync for user=\(userId)")

		// Don't run the initial seed until HealthKit authorization is granted; otherwise we export a near-empty CSV.
		if !healthStoreService.isReadGrantedCached() {
			print("[HealthSync] HealthKit not authorized yet; deferring initial seed until authorization completes")
			pendingSeedUserId = userId
			return
		}

		// Avoid duplicate seed scheduling during the same app session.
		if isPerformingInitialSeed || initialSeedWorkScheduled {
			print("[HealthSync] Initial seed already in progress/scheduled, skipping duplicate start")
			return
		}

		// Check if initial seed already done for this user
		let seedKey = self.initialSeedDoneKey(for: userId)
		let seedBytesKey = self.initialSeedBytesKey(for: userId)
		let seedAuthKey = self.initialSeedAuthorizedKey(for: userId)
		if UserDefaults.standard.bool(forKey: seedKey) {
			// If we've already successfully run a seed *after* HealthKit auth, don't rerun (even if user has 0 data).
			if UserDefaults.standard.bool(forKey: seedAuthKey) {
				print("[HealthSync] Initial seed already completed for user=\(userId), skipping")
				return
			}
			let prevBytes = UserDefaults.standard.integer(forKey: seedBytesKey)
			print("[HealthSync] Initial seed was previously marked done but was not confirmed post-auth (\(prevBytes) bytes); re-running")
		}

		// Set flag to prevent observer uploads during initial seed
		self.isPerformingInitialSeed = true
		self.initialSeedWorkScheduled = true

		print("[HealthSync] Performing initial seed for user=\(userId)")

		// Delay initial seed to let any ongoing operations settle
		let initialSeed = DispatchWorkItem { [weak self] in
			guard let self = self else { return }

			// Mirror seed: export raw samples in manageable chunks to avoid oversized uploads/timeouts.
			let now = Date()
			let backfillDays = 60
			// Split the seed window into smaller uploads for reliability on mobile networks.
			let chunkDays = 7
			guard let startAllRaw = Calendar.current.date(byAdding: .day, value: -backfillDays, to: now) else {
				self.isPerformingInitialSeed = false
				self.initialSeedWorkScheduled = false
				return
			}
			// Align to local day boundaries so chunk windows don't start mid-day.
			let startAll = Calendar.current.startOfDay(for: startAllRaw)

			Task {
				do {
					let seedBatchId = UUID().uuidString
					var cursor = startAll
					var totalBytes = 0
					var lastTaskId: String?
					let chunkTotal = Int(ceil(Double(backfillDays) / Double(chunkDays)))
					var chunkIndex = 0
					while cursor < now {
						chunkIndex += 1
						let next = Calendar.current.date(byAdding: .day, value: chunkDays, to: cursor) ?? now
						let end = min(next, now)
						let data: Data = try await withCheckedThrowingContinuation { cont in
							HealthCSVExporter.generateMirrorCSV(for: userId, start: cursor, end: end) { res in
								switch res {
								case .success(let d): cont.resume(returning: d)
								case .failure(let e): cont.resume(throwing: e)
								}
							}
						}
						totalBytes += data.count

						print("[HealthSync] Uploading mirror seed chunk \(cursor) -> \(end) (\(data.count) bytes)")
						let fileName = "health-seed-\(seedBatchId)-\(chunkIndex)-of-\(chunkTotal).csv"
						lastTaskId = try await AgentBackendService.shared.uploadHealthCSV(
							data,
							uploadMode: "seed",
							fileName: fileName,
							seedBatchId: seedBatchId,
							seedChunkIndex: chunkIndex,
							seedChunkTotal: chunkTotal
						)

						// Small delay to avoid bursty uploads.
						try await Task.sleep(nanoseconds: 250_000_000)
						cursor = end
					}

					await MainActor.run {
						self.isPerformingInitialSeed = false
						self.initialSeedWorkScheduled = false
						UserDefaults.standard.set(true, forKey: seedKey)
						UserDefaults.standard.set(totalBytes, forKey: seedBytesKey)
						UserDefaults.standard.set(true, forKey: seedAuthKey)
					}

					if let taskId = lastTaskId, taskId.isEmpty == false {
						Task.detached(priority: .background) {
							do {
								let status = try await AgentBackendService.shared.waitForHealthTask(taskId, timeout: 600, pollInterval: 3)
								print("[HealthSync] Mirror seed last-chunk backend state=\(status.state)")
							} catch {
								print("[HealthSync] Mirror seed status poll failed (non-fatal): \(error)")
							}
						}
					}
				} catch {
					print("[HealthSync] Mirror seed failed: \(error)")
					await MainActor.run {
						self.isPerformingInitialSeed = false
						self.initialSeedWorkScheduled = false
					}
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

		// Capture timezone + location whenever we get woken up for HealthKit background delivery.
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
		// Use anchored queries for tight raw-sample deltas (HealthKit mirror).
		let now = Date()
		let group = DispatchGroup()
		let lock = NSLock()
		var collectedSamples: [HKSample] = []
		var collectedDeleted: [HKDeletedObject] = []

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
				// We still establish anchors, but we don't upload a huge delta here.
				if anyMissingAnchor {
					return
				}

				let samples = samplesOrNil ?? []
				if !samples.isEmpty {
					lock.lock()
					collectedSamples.append(contentsOf: samples)
					lock.unlock()
				}
				if let deleted = deleted, !deleted.isEmpty {
					lock.lock()
					collectedDeleted.append(contentsOf: deleted)
					lock.unlock()
				}
			}
			healthStore.execute(anchored)
		}

		group.notify(queue: .main) { [weak self] in
			guard let self = self else { return }

			if anyMissingAnchor {
				print("[HealthSync] Anchors missing; established anchors but skipping huge delta upload")
				return
			}

			let samples = collectedSamples
			let deleted = collectedDeleted
			if samples.isEmpty && deleted.isEmpty {
				return
			}

			Task {
				var userId = self.currentUserId
				if userId == nil {
					userId = try? await AuthService.getUserId()
					self.currentUserId = userId
				}
				guard let uid = userId else { return }

				HealthCSVExporter.generateMirrorDeltaCSV(for: uid, samples: samples, deleted: deleted) { result in
					switch result {
					case .success(let data):
						Task {
							do {
								print("[HealthSync] Uploading mirror delta CSV (\(data.count) bytes) samples=\(samples.count) deleted=\(deleted.count)")
								let taskId = try await AgentBackendService.shared.uploadHealthCSV(data, uploadMode: "delta")
								print("[HealthSync] Mirror delta upload enqueued with task_id=\(taskId)")
							} catch {
								print("[HealthSync] Mirror delta upload failed: \(error)")
							}
						}
					case .failure(let error):
						print("[HealthSync] Failed to generate mirror delta CSV: \(error)")
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


