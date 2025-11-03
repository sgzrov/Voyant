//
//  TagView.swift
//  HealthPredictor
//
//  Created by Stephan  on 27.05.2025.
//

import SwiftUI

struct TagView: View {
    let tag: Tag

    var body: some View {
        HStack(spacing: 4) {
            Circle()
                .fill(tag.color)
                .frame(width: 6, height: 6)

            Text(tag.name)
                .font(.footnote)
                .fontWeight(.medium)
                .foregroundColor(tag.color)
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 6)
        .background(
            RoundedRectangle(cornerRadius: 8)
                .fill(tag.color.opacity(0.12))
        )
        .overlay(
            RoundedRectangle(cornerRadius: 8)
                .stroke(tag.color.opacity(0.3), lineWidth: 1)
        )
    }
}

#Preview {
    TagView(tag: Tag(name: "Sleep", color: .blue, subtags: []))
}
