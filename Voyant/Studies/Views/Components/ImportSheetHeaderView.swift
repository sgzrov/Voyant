import SwiftUI

struct ImportSheetHeaderView: View {

    @Binding var showFileImporter: Bool

    var onDismiss: () -> Void

    var body: some View {
        HStack {
            Button(action: {
                showFileImporter = true
            }) {
                ZStack {
                    Circle()
                        .fill(Color(.secondarySystemFill))
                        .frame(width: 36, height: 36)
                    Image(systemName: "folder")
                        .resizable()
                        .frame(width: 19, height: 16)
                        .foregroundColor(Color.accentColor)
                }
            }
            Spacer()
            Button(action: onDismiss) {
                ZStack {
                    Circle()
                        .fill(Color(.secondarySystemFill))
                        .frame(width: 30, height: 30)
                    Image(systemName: "xmark")
                        .resizable()
                        .frame(width: 12, height: 12)
                        .foregroundColor(Color(.systemGroupedBackground))
                }
            }
        }
        .padding(.top, 6)
    }
}