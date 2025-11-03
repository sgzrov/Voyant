import SwiftUI

struct ImportSheetInputSection: View {

    @ObservedObject var importVM: TagExtractionViewModel

    @Binding var selectedFileURL: URL?

    @FocusState.Binding var isTextFieldFocused: Bool

    var body: some View {
        Group {
            if selectedFileURL == nil {
                VStack {
                    HStack {
                        Image(systemName: "link")
                            .foregroundColor(Color(.tertiaryLabel))
                        TextField("Paste URL here", text: $importVM.importInput)
                            .focused($isTextFieldFocused)
                            .autocapitalization(.none)
                            .disableAutocorrection(true)
                            .truncationMode(.middle)
                            .onChange(of: importVM.importInput) { oldValue, newValue in
                                importVM.validateURL()
                                if importVM.isFullyValidURL(), let url = URL(string: newValue) {
                                    Task {
                                        await importVM.validateFileType(url: url)
                                    }
                                }
                            }
                        if !importVM.importInput.isEmpty {
                            Button(action: {
                                importVM.clearInput()
                            }) {
                                Image(systemName: "xmark.circle.fill")
                                    .foregroundColor(.gray)
                            }
                        }
                    }
                    .padding(12)
                    .background(Color(.secondarySystemFill))
                    .cornerRadius(12)
                    .padding(.top, 40)
                    if !importVM.errorMessage.isEmpty {
                        Text(importVM.errorMessage)
                            .foregroundColor(.red)
                            .font(.footnote)
                            .frame(maxWidth: .infinity, alignment: .leading)
                            .padding(.leading, 2)
                            .padding(.top, 6)
                    }
                }
            } else if let fileURL = selectedFileURL {
                let attrs = try? FileManager.default.attributesOfItem(atPath: fileURL.path)
                let fileSize = attrs?[.size] as? UInt64
                let fileDate = attrs?[.modificationDate] as? Date
                let dateString: String = {
                    guard let fileDate else { return "" }
                    let formatter = DateFormatter()
                    formatter.dateFormat = "MM.dd.yyyy"
                    return formatter.string(from: fileDate)
                }()
                let sizeString: String = {
                    guard let fileSize else { return "" }
                    if fileSize < 1024 {
                        return "\(fileSize) bytes"
                    } else if fileSize < 1024 * 1024 {
                        return String(format: "%.1f KB", Double(fileSize) / 1024.0)
                    } else {
                        return String(format: "%.2f MB", Double(fileSize) / (1024.0 * 1024.0))
                    }
                }()
                HStack(alignment: .firstTextBaseline, spacing: 12) {
                    Image(systemName: "folder.badge.plus")
                        .resizable()
                        .scaledToFit()
                        .frame(width: 40, height: 40)
                        .foregroundColor(.accentColor)
                        .alignmentGuide(.firstTextBaseline) { dimensions in
                            dimensions[VerticalAlignment.center] - 0.5
                        }
                    VStack(alignment: .leading, spacing: 10) {
                        Text(fileURL.lastPathComponent)
                            .font(.subheadline)
                            .fontWeight(.semibold)
                            .foregroundColor(.primary)
                        Text("\(dateString) - \(sizeString)")
                            .font(.footnote)
                            .foregroundColor(.secondary.opacity(0.6))
                    }
                    Spacer()
                }
                .padding()
                .background(Color(.secondarySystemFill))
                .cornerRadius(12)
                .padding(.top, 40)
                .transition(.move(edge: .bottom).combined(with: .opacity))
            }
        }
    }
}