//
//  ModelOption.swift
//  Voyant
//
//  Defines selectable model options and their provider/model identifiers.
//

import Foundation

enum ModelOption: String, CaseIterable, Identifiable, Codable {
    case openai_gpt5mini
    case grok_grok4fast
    case anthropic_claudesonnet45
    case gemini_gemini25flash

    var id: String { rawValue }

    var displayName: String {
        switch self {
        case .openai_gpt5mini: return "GPT‑5 mini (OpenAI)"
        case .grok_grok4fast: return "Grok‑4 Fast (xAI)"
        case .anthropic_claudesonnet45: return "Claude Sonnet 4.5 (Anthropic)"
        case .gemini_gemini25flash: return "Gemini 2.5 Flash Lite (Google)"
        }
    }

    var providerId: String {
        switch self {
        case .openai_gpt5mini: return "openai"
        case .grok_grok4fast: return "grok"
        case .anthropic_claudesonnet45: return "anthropic"
        case .gemini_gemini25flash: return "gemini"
        }
    }

    var modelId: String {
        switch self {
        case .openai_gpt5mini: return "gpt-5-mini"
        case .grok_grok4fast: return "grok-4-fast"
        case .anthropic_claudesonnet45: return "claude-sonnet-4-5"
        case .gemini_gemini25flash: return "gemini-2.5-flash-lite"
        }
    }
}


