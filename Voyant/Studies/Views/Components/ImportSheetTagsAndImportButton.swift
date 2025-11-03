import SwiftUI

struct ImportSheetTagsAndImportButton: View {

    @ObservedObject var importVM: TagExtractionViewModel

    @Binding var selectedFileURL: URL?

    @FocusState.Binding var isTextFieldFocused: Bool

    var onImport: () -> Void

    var body: some View {
        VStack {
            if !importVM.topTags.isEmpty && importVM.errorMessage.isEmpty && (importVM.isFullyValidURL() || selectedFileURL != nil) {
                HStack(spacing: 8) {
                    ForEach(importVM.visibleTags.indices, id: \ .self) { idx in
                        TagView(tag: importVM.visibleTags[idx])
                            .transition(.scale.combined(with: .opacity))
                            .onAppear {
                                if idx == importVM.visibleTags.count - 1 {
                                    isTextFieldFocused = false
                                }
                            }
                    }
                }
                .frame(maxWidth: .infinity, alignment: .leading)
                .padding(.horizontal, 2)
                .padding(.vertical, 12)
                .animation(.spring(response: 0.3, dampingFraction: 0.7), value: importVM.visibleTags)
            }
            Spacer()
            Button(action: onImport) {
                Text("Import")
                    .font(.headline)
                    .foregroundColor(.white)
                    .frame(maxWidth: .infinity)
                    .padding()
                    .background(Color.accentColor)
                    .cornerRadius(14)
            }
            .disabled(
                importVM.isLoading ||
                importVM.isExtractingTags ||
                importVM.visibleTags.count < 1 ||
                (importVM.importInput.isEmpty && selectedFileURL == nil) ||
                (!importVM.isFullyValidURL() && selectedFileURL == nil) ||
                !importVM.errorMessage.isEmpty
            )
            .opacity(
                importVM.isLoading ||
                importVM.isExtractingTags ||
                importVM.visibleTags.count < 1 ||
                (importVM.importInput.isEmpty && selectedFileURL == nil) ||
                (!importVM.isFullyValidURL() && selectedFileURL == nil) ||
                !importVM.errorMessage.isEmpty
                ? 0.5 : 1.0
            )
            .padding(.bottom, 24)
        }
    }
}