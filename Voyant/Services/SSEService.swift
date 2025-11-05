//
//  SSEService.swift
//  HealthPredictor
//
//  Created by Stephan on 22.06.2025.
//

import Foundation
import Combine

// Import the protocol from the Extensions directory

enum SSEError: Error {
    case httpError(Int, String)
    case serverError(String)
    case connectionError(String)
}

enum SSEEventType {
    case message
    case error
    case done
}

struct SSEEvent {
    let type: SSEEventType
    let data: String
    let id: String?
}

struct StreamingChunk: Codable {
    let content: String?
    let done: Bool
    let error: String?
}

class SSEService: NSObject, URLSessionDataDelegate {

    static let shared = SSEService()

    private var session: URLSession!

    private override init() {
        super.init()
        let config = URLSessionConfiguration.default
        config.timeoutIntervalForRequest = 120
        config.timeoutIntervalForResource = 600
        session = URLSession(configuration: config, delegate: self, delegateQueue: nil)
    }

    func streamSSE(request: URLRequest) async throws -> AsyncStream<String> {
        print("[SSEService] Starting SSE stream with URL: \(request.url?.absoluteString ?? "nil")")
        print("[SSEService] Request headers: \(request.allHTTPHeaderFields ?? [:])")
        return AsyncStream<String> { continuation in
            let delegate = SSEStreamDelegate(continuation: continuation)
            let config = URLSessionConfiguration.default
            config.timeoutIntervalForRequest = 120
            config.timeoutIntervalForResource = 600
            let session = URLSession(configuration: config, delegate: delegate, delegateQueue: nil)

            var modifiedRequest = request
            modifiedRequest.setValue("text/event-stream", forHTTPHeaderField: "Accept")
            modifiedRequest.setValue("no-cache", forHTTPHeaderField: "Cache-Control")
            modifiedRequest.setValue("keep-alive", forHTTPHeaderField: "Connection")
            if let contentType = request.value(forHTTPHeaderField: "Content-Type") {
                modifiedRequest.setValue(contentType, forHTTPHeaderField: "Content-Type")
            }

            print("[SSEService] Initiating data task...")
            delegate.dataTask = session.dataTask(with: modifiedRequest)
            delegate.dataTask?.resume()

            let cancellable = delegate.eventPublisher
                .sink(
                    receiveCompletion: { completion in
                        switch completion {
                        case .finished:
                            print("[SSEService] Stream finished.")
                            continuation.finish()
                        case .failure(let error):
                            print("[SSEService] Stream error: \(error.localizedDescription)")
                            continuation.yield("Error: \(error.localizedDescription)")
                            continuation.finish()
                        }
                    },
                    receiveValue: { event in
                        print("[SSEService] Received event: \(event)")
                        delegate.handleSSEEvent(event)
                    }
                )

            continuation.onTermination = { _ in
                print("[SSEService] Stream terminated by consumer.")
                cancellable.cancel()
                delegate.dataTask?.cancel()
            }
        }
    }
}

// Helper delegate class for per-stream state
class SSEStreamDelegate: NSObject, URLSessionDataDelegate {
    let eventPublisher = PassthroughSubject<SSEEvent, Error>()
    var eventBuffer: String = ""
    let continuation: AsyncStream<String>.Continuation
    var dataTask: URLSessionDataTask?

    init(continuation: AsyncStream<String>.Continuation) {
        self.continuation = continuation
    }

    func urlSession(_ session: URLSession, dataTask: URLSessionDataTask, didReceive data: Data) {
        guard let receivedString = String(data: data, encoding: .utf8) else {
            print("[SSEStreamDelegate] Received non-UTF8 data chunk.")
            return
        }
        print("[SSEStreamDelegate] Received data chunk: \(receivedString.prefix(200))")
        if receivedString.contains("\"error\"") && !receivedString.contains("data:") {
            let error = NSError(domain: "SSE", code: -1, userInfo: [NSLocalizedDescriptionKey: "Backend error: \(receivedString)"])
            eventPublisher.send(completion: .failure(error))
            return
        }
        eventBuffer += receivedString
        processCompleteEvents()
    }

    func urlSession(_ session: URLSession, task: URLSessionTask, didCompleteWithError error: Error?) {
        if let error = error {
            eventPublisher.send(completion: .failure(error))
        } else {
            eventPublisher.send(completion: .finished)
        }
    }

    private func processCompleteEvents() {
        if eventBuffer.contains("\n\n") {
            let events = eventBuffer.components(separatedBy: "\n\n")
            if events.count > 1 {
                eventBuffer = events.last ?? ""
                for eventString in events.dropLast() where !eventString.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                    print("[SSEStreamDelegate] Parsing event string: \(eventString.prefix(200))")
                    parseEvent(eventString)
                }
            }
        }
    }

    private func parseEvent(_ eventString: String) {
        var eventType: SSEEventType = .message
        var eventData = ""
        var eventId: String?
        let lines = eventString.components(separatedBy: "\n")
        for line in lines {
            if line.hasPrefix("event:") {
                let type = line.dropFirst(6).trimmingCharacters(in: .whitespaces)
                switch type {
                case "error":
                    eventType = .error
                case "done":
                    eventType = .done
                default:
                    eventType = .message
                }
            } else if line.hasPrefix("data:") {
                eventData = line.dropFirst(5).trimmingCharacters(in: .whitespaces)
            } else if line.hasPrefix("id:") {
                eventId = line.dropFirst(3).trimmingCharacters(in: .whitespaces)
            }
        }
        print("[SSEStreamDelegate] Parsed eventType: \(eventType), eventId: \(eventId ?? "nil"), eventData: \(eventData.prefix(200)))")
        if !eventData.isEmpty {
            let event = SSEEvent(type: eventType, data: eventData, id: eventId)
            eventPublisher.send(event)
        }
    }

    func handleSSEEvent(_ event: SSEEvent) {
        print("[SSEStreamDelegate] Handling SSEEvent: type=\(event.type), id=\(event.id ?? "nil"), data=\(event.data.prefix(200)))")
        switch event.type {
        case .message:
            // If the server provides a conversation_id in the event payload, forward the raw JSON
            // so the consumer can extract and persist it before streaming content arrives.
            if event.data.contains("\"conversation_id\"") {
                continuation.yield(event.data)
                return
            }
            let jsonData = Data(event.data.utf8)
            if let chunk = try? JSONDecoder().decode(StreamingChunk.self, from: jsonData) {
                if let error = chunk.error {
                    print("[SSEStreamDelegate] StreamingChunk error: \(error)")
                    continuation.yield("Error: \(error)")
                    return
                }
                if let content = chunk.content, !content.isEmpty {
                    print("[SSEStreamDelegate] StreamingChunk content: \(content.prefix(200)))")
                    continuation.yield(content)
                }
                if chunk.done {
                    print("[SSEStreamDelegate] StreamingChunk done.")
                    continuation.finish()
                }
            } else {
                if event.data.contains("\"content\":") {
                    if let contentStart = event.data.range(of: "\"content\":\"")?.upperBound,
                       let contentEnd = event.data[contentStart...].range(of: "\"")?.lowerBound {
                        let content = String(event.data[contentStart..<contentEnd])
                        print("[SSEStreamDelegate] Fallback content parse: \(content.prefix(200)))")
                        continuation.yield(content)
                    }
                }
            }
        case .error:
            print("[SSEStreamDelegate] SSEEvent error: \(event.data)")
            continuation.yield("Error: \(event.data)")
        case .done:
            print("[SSEStreamDelegate] SSEEvent done.")
            continuation.finish()
        }
    }
}

extension SSEStreamDelegate {
    func urlSession(_ session: URLSession, task: URLSessionTask, didReceive response: URLResponse, completionHandler: @escaping (URLSession.ResponseDisposition) -> Void) {
        if let httpResponse = response as? HTTPURLResponse {
            print("[SSEStreamDelegate] Received HTTP response: \(httpResponse.statusCode)")
            if httpResponse.statusCode >= 400 {
                let error = NSError(domain: "SSE", code: httpResponse.statusCode, userInfo: [NSLocalizedDescriptionKey: "HTTP \(httpResponse.statusCode)"])
                eventPublisher.send(completion: .failure(error))
                completionHandler(.cancel)
                return
            }
        }
        completionHandler(.allow)
    }
}