//
//  CSVManager.swift
//  HealthPredictor
//
//  Created by Stephan  on 18.06.2025.
//

import Foundation
import HealthKit

class UserFileCreationService: UserFileCreationServiceProtocol {

    static let shared = UserFileCreationService()

    private let healthStoreService: HealthStoreServiceProtocol

    private init() {
        self.healthStoreService = HealthStoreService.shared
    }
    private let fileName = "user_health_data.csv"

    private static let dateFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyy-MM-dd"
        return formatter
    }()

    private static let monthFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyy-MM"
        return formatter
    }()

    private static let hourFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateFormat = "HH:00"
        return formatter
    }()

    func generateCSV(completion: @escaping (URL?) -> Void) {
        let metricNames = Self.metricNames()
        let header = (["Date", "Time"] + metricNames).joined(separator: ",")

        fetchAllMetricsData { dailyRows, monthlyRows in
            var csvRows: [String] = [header]
            csvRows.append(contentsOf: dailyRows)
            csvRows.append(contentsOf: monthlyRows)
            let csvString = csvRows.joined(separator: "\n")
            let fileURL = self.writeCSVToFile(csvString: csvString)
            completion(fileURL)
        }
    }

    static func metricNames() -> [String] {
        let quantityNames = Array(HealthMetricMapper.quantitySubtagToType.keys)
        let categoryNames = Array(HealthMetricMapper.categorySubtagToType.keys)
        return quantityNames + categoryNames
    }

    private func fetchAllMetricsData(completion: @escaping ([String], [String]) -> Void) {
        let metricNames = Self.metricNames()
        let today = Date()
        let calendar = Calendar.current
        let group = DispatchGroup()

        // hourlyData[dateString][timeString][metric] = value
        var hourlyData: [String: [String: [String: Double]]] = [:]

        // Build date range
        let dailyDates: [Date] = (0..<180).compactMap { calendar.date(byAdding: .day, value: -$0, to: today) }

        for metric in metricNames {
            if let quantityType = HealthMetricMapper.quantityType(for: metric),
               let hkType = HKObjectType.quantityType(forIdentifier: quantityType) {
                group.enter()
                fetchQuantityDataHourly(
                    type: hkType,
                    metric: metric,
                    dailyDates: dailyDates
                ) { perDayHourly in
                    for (dateStr, timeValues) in perDayHourly {
                        var timeDict = hourlyData[dateStr, default: [:]]
                        for (timeStr, value) in timeValues {
                            var metricDict = timeDict[timeStr, default: [:]]
                            metricDict[metric] = value
                            timeDict[timeStr] = metricDict
                        }
                        hourlyData[dateStr] = timeDict
                    }
                    group.leave()
                }
            } else if let categoryType = HealthMetricMapper.categoryType(for: metric),
                      let hkType = HKObjectType.categoryType(forIdentifier: categoryType) {
                group.enter()
                fetchCategoryDataHourly(
                    type: hkType,
                    metric: metric,
                    dailyDates: dailyDates
                ) { perDayHourly in
                    for (dateStr, timeValues) in perDayHourly {
                        var timeDict = hourlyData[dateStr, default: [:]]
                        for (timeStr, value) in timeValues {
                            var metricDict = timeDict[timeStr, default: [:]]
                            metricDict[metric] = value
                            timeDict[timeStr] = metricDict
                        }
                        hourlyData[dateStr] = timeDict
                    }
                    group.leave()
                }
            }
        }

        group.notify(queue: .main) {
            let sortedDaily = dailyDates.map { Self.dateFormatter.string(from: $0) }
            let timesDesc: [String] = (0..<24).reversed().map { String(format: "%02d:00", $0) }
            var dailyRows: [String] = []
            for dateStr in sortedDaily {
                for timeStr in timesDesc {
                    let values = metricNames.map { metric in
                        if let val = hourlyData[dateStr]?[timeStr]?[metric] {
                            return String(format: "%.2f", val)
                        } else {
                            return ""
                        }
                    }
                    dailyRows.append(([dateStr, timeStr] + values).joined(separator: ","))
                }
            }

            completion(dailyRows, [])
        }
    }

    private func fetchQuantityData(type: HKQuantityType, metric: String, dailyDates: [Date], completion: @escaping ([String: Double]) -> Void) {
        let calendar = Calendar.current
        let now = Date()
        let startDate = calendar.date(byAdding: .day, value: -149, to: now) ?? now
        let anchorDate = calendar.startOfDay(for: startDate)
        let dailyInterval = DateComponents(day: 1)
        let predicate = HKQuery.predicateForSamples(withStart: calendar.date(byAdding: .year, value: -2, to: now), end: now, options: .strictStartDate)
        let unit = HKUnit(from: HealthMetricMapper.unit(for: metric))
        let statsOption = HealthMetricMapper.statisticsOption(for: metric)

        var dailyResults: [String: Double] = [:]

        let dailyQuery = HKStatisticsCollectionQuery(quantityType: type, quantitySamplePredicate: predicate, options: statsOption, anchorDate: anchorDate, intervalComponents: dailyInterval)
        dailyQuery.initialResultsHandler = { [weak self] _, results, error in
            if let error = error {
                print("Error fetching daily \(metric): \(error.localizedDescription)")
                completion(dailyResults)
                return
            }

            if let statsCollection = results {
                for date in dailyDates {
                    let stat = statsCollection.statistics(for: date)
                    let value = self?.extractQuantityValue(stat: stat, unit: unit, statsOption: statsOption)
                    if let value = value {
                        dailyResults[Self.dateFormatter.string(from: date)] = value
                    }
                }
            }
            completion(dailyResults)
        }
        self.healthStoreService.healthStore.execute(dailyQuery)
    }

    private func extractQuantityValue(stat: HKStatistics?, unit: HKUnit, statsOption: HKStatisticsOptions) -> Double? {
        if statsOption == .cumulativeSum {
            return stat?.sumQuantity()?.doubleValue(for: unit)
        } else {
            return stat?.averageQuantity()?.doubleValue(for: unit)
        }
    }

    private func fetchQuantityDataHourly(type: HKQuantityType, metric: String, dailyDates: [Date], completion: @escaping ([String: [String: Double]]) -> Void) {
        let calendar = Calendar.current
        let today = Date()
        guard let startDay = dailyDates.last else { completion([:]); return }
        let rangeStart = calendar.startOfDay(for: startDay)
        let rangeEnd = calendar.date(byAdding: .day, value: 1, to: calendar.startOfDay(for: dailyDates.first ?? today)) ?? today

        let predicate = HKQuery.predicateForSamples(withStart: rangeStart, end: rangeEnd, options: .strictStartDate)
        let unitStr = HealthMetricMapper.unit(for: metric)
        let unit = HKUnit(from: unitStr)
        let statsOption = HealthMetricMapper.statisticsOption(for: metric)

        var resultsPerDay: [String: [String: Double]] = [:]

        let interval = DateComponents(hour: 1)
        let anchorDate = calendar.startOfDay(for: rangeStart)
        let query = HKStatisticsCollectionQuery(quantityType: type, quantitySamplePredicate: predicate, options: statsOption, anchorDate: anchorDate, intervalComponents: interval)
        query.initialResultsHandler = { [weak self] _, results, error in
            if let error = error {
                print("Error fetching hourly \(metric): \(error.localizedDescription)")
                completion(resultsPerDay)
                return
            }

            if let statsCollection = results {
                statsCollection.enumerateStatistics(from: rangeStart, to: rangeEnd) { stat, _ in
                    guard let value = self?.extractQuantityValue(stat: stat, unit: unit, statsOption: statsOption) else { return }
                    let dateStr = Self.dateFormatter.string(from: stat.startDate)
                    let timeStr = Self.hourFormatter.string(from: stat.startDate)
                    var perHour = resultsPerDay[dateStr, default: [:]]
                    perHour[timeStr] = value
                    resultsPerDay[dateStr] = perHour
                }
            }
            completion(resultsPerDay)
        }
        self.healthStoreService.healthStore.execute(query)
    }

    private func fetchCategoryDataHourly(type: HKCategoryType, metric: String, dailyDates: [Date], completion: @escaping ([String: [String: Double]]) -> Void) {
        let calendar = Calendar.current
        let today = Date()
        guard let startDay = dailyDates.last else { completion([:]); return }
        let rangeStart = calendar.startOfDay(for: startDay)
        let rangeEnd = calendar.date(byAdding: .day, value: 1, to: calendar.startOfDay(for: dailyDates.first ?? today)) ?? today

        let predicate = HKQuery.predicateForSamples(withStart: rangeStart, end: rangeEnd, options: .strictStartDate)

        var perDayHourResults: [String: [String: Double]] = [:]

        let query = HKSampleQuery(sampleType: type, predicate: predicate, limit: HKObjectQueryNoLimit, sortDescriptors: nil) { _, samples, error in
            if let error = error {
                print("Error fetching category \(metric): \(error.localizedDescription)")
                completion(perDayHourResults)
                return
            }

            guard let samples = samples as? [HKCategorySample] else {
                completion(perDayHourResults)
                return
            }

            for date in dailyDates {
                let dayStart = calendar.startOfDay(for: date)
                for hour in 0..<24 {
                    let hourStart = calendar.date(byAdding: .hour, value: hour, to: dayStart) ?? dayStart
                    let hourEnd = calendar.date(byAdding: .hour, value: 1, to: hourStart) ?? hourStart
                    let overlapping = samples.filter { $0.endDate > hourStart && $0.startDate < hourEnd }
                    if !overlapping.isEmpty {
                        let total = self.calculateCategoryOverlapTotal(samples: overlapping, metric: metric, hourStart: hourStart, hourEnd: hourEnd)
                        if total > 0 {
                            let dateStr = Self.dateFormatter.string(from: dayStart)
                            let timeStr = String(format: "%02d:00", hour)
                            var perHour = perDayHourResults[dateStr, default: [:]]
                            perHour[timeStr] = total
                            perDayHourResults[dateStr] = perHour
                        }
                    }
                }
            }
            completion(perDayHourResults)
        }
        self.healthStoreService.healthStore.execute(query)
    }

    private func calculateCategoryOverlapTotal(samples: [HKCategorySample], metric: String, hourStart: Date, hourEnd: Date) -> Double {
        let overlappedSeconds = samples.reduce(0.0) { partial, s in
            let start = max(s.startDate, hourStart)
            let end = min(s.endDate, hourEnd)
            let interval = end.timeIntervalSince(start)
            return interval > 0 ? partial + interval : partial
        }
        if metric == "Sleep Duration" {
            return overlappedSeconds / 3600.0
        } else if metric == "Mindfulness Minutes" {
            return overlappedSeconds / 60.0
        }
        fatalError("Unknown category metric: \(metric)")
    }

    private func writeCSVToFile(csvString: String) -> URL? {
        let fileManager = FileManager.default
        guard let docsURL = fileManager.urls(for: .documentDirectory, in: .userDomainMask).first else { return nil }
        let fileURL = docsURL.appendingPathComponent(fileName)
        do {
            try csvString.write(to: fileURL, atomically: true, encoding: .utf8)
            return fileURL
        } catch {
            print("Failed to write CSV: \(error)")
            return nil
        }
    }
}
