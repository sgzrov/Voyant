//
//  HealthSyncService.swift
//  Voyant
//

import Foundation
import HealthKit

final class HealthSyncService {

	static let shared = HealthSyncService()
	private init() {}

	private let healthStore = HKHealthStore()
	private let calendar = Calendar.current
	private var currentUserId: String?
	private let lookbackHours: Int = 6

	// Call on app launch after permissions are granted
	func startBackgroundSync(userId: String) {
		// Avoid double‑starting
		guard currentUserId == nil else { return }
		self.currentUserId = userId
		print("[HealthSync] startBackgroundSync for user=\(userId)")
		// Ensure HK permission is granted before attempting queries
        HealthStoreService.shared.requestAuthorization { [weak self] (ok: Bool, err: Error?) in
			guard let self = self else { return }
			if !ok {
				print("[HealthSync] HealthKit authorization not granted: \(err?.localizedDescription ?? "unknown")")
				return
			}
			print("[HealthSync] HealthKit authorized, starting observers and initial seed")
			self.registerObservers()
			self.enableBackgroundDelivery()
			// Give HealthKit a short moment to finalize auth state before heavy queries
			let initialSeed = DispatchWorkItem {
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
								let taskId = try await AgentBackendService.shared.uploadHealthCSV(data)
								print("[HealthSync] Enqueued process_csv_upload taskId=\(taskId)")
								// Optionally poll for completion so we can log success
								let status = try await AgentBackendService.shared.waitForHealthTask(taskId, timeout: 120)
								print("[HealthSync] Initial seed completed with state=\(status.state)")
							} catch {
								print("[HealthSync] Initial upload failed: \(error)")
							}
						}
					case .failure(let e):
						print("[HealthSync] generateCSV failed: \(e.localizedDescription)")
					}
				}
			}
			DispatchQueue.main.asyncAfter(deadline: .now() + 0.5, execute: initialSeed)
		}
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
			guard let self = self else { completionHandler(); return }
			let end = now
			let start: Date = {
				if let minChangedDate = minChangedDate {
					return min(minChangedDate, now.addingTimeInterval(-Double(self.lookbackHours) * 3600.0))
				}
				return now.addingTimeInterval(-Double(self.lookbackHours) * 3600.0)
			}()

			func finish() { completionHandler() }

			Task {
				var userId = self.currentUserId
				if userId == nil {
					userId = try? await AuthService.getUserId()
					self.currentUserId = userId
				}
				guard let uid = userId else { finish(); return }

				let minuteResolution = end.timeIntervalSince(start) <= (90 * 60)
				HealthCSVExporter.generateDeltaCSV(for: uid, start: start, end: end, metrics: [], minuteResolution: minuteResolution) { result in
					switch result {
					case .success(let data):
						// Save delta CSV for debugging
						do {
							let ts = ISO8601DateFormatter().string(from: Date()).replacingOccurrences(of: ":", with: "-")
							if let docs = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask).first {
								let url = docs.appendingPathComponent("HealthDelta-\(ts).csv")
								try data.write(to: url, options: .atomic)
								print("[HealthSync] Saved delta CSV to \(url.path)")
							}
						} catch {
							print("[HealthSync] Failed saving delta CSV: \(error)")
						}
						Task {
							_ = try? await AgentBackendService.shared.uploadHealthCSV(data)
							finish()
						}
					case .failure:
						finish()
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


