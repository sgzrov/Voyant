import Foundation
import CryptoKit

extension Data {
    /// Generate SHA256 hash of the data as a hex string
    func sha256Hash() -> String {
        let hash = SHA256.hash(data: self)
        return hash.compactMap { String(format: "%02x", $0) }.joined()
    }
}
