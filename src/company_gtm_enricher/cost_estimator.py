from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable, List


@dataclass(frozen=True)
class PricingModel:
    provider: str
    model: str
    currency: str
    input_price_per_million_tokens: float
    output_price_per_million_tokens: float


@dataclass(frozen=True)
class CostEstimatorInputs:
    company_count: int
    batch_size: int
    shared_input_tokens_per_request: int
    input_tokens_per_company: int
    output_tokens_per_company: int


@dataclass(frozen=True)
class CostEstimate:
    provider: str
    model: str
    currency: str
    request_count: int
    total_input_tokens: int
    total_output_tokens: int
    estimated_input_cost: float
    estimated_output_cost: float
    estimated_total_cost: float


DEFAULT_PRICING_MODELS: List[PricingModel] = [
    PricingModel(
        provider="OpenAI",
        model="GPT-5",
        currency="USD",
        input_price_per_million_tokens=1.25,
        output_price_per_million_tokens=10.00,
    ),
    PricingModel(
        provider="OpenAI",
        model="GPT-5 mini",
        currency="USD",
        input_price_per_million_tokens=0.25,
        output_price_per_million_tokens=2.00,
    ),
    PricingModel(
        provider="Anthropic",
        model="Claude Sonnet 4.5",
        currency="USD",
        input_price_per_million_tokens=3.00,
        output_price_per_million_tokens=15.00,
    ),
    PricingModel(
        provider="Anthropic",
        model="Claude Haiku 4.5",
        currency="USD",
        input_price_per_million_tokens=1.00,
        output_price_per_million_tokens=5.00,
    ),
    PricingModel(
        provider="MiniMax",
        model="MiniMax-M2.5",
        currency="CNY",
        input_price_per_million_tokens=2.10,
        output_price_per_million_tokens=8.40,
    ),
    PricingModel(
        provider="MiniMax",
        model="MiniMax-M2.5-highspeed",
        currency="CNY",
        input_price_per_million_tokens=4.20,
        output_price_per_million_tokens=16.80,
    ),
]


def estimate_costs(
    inputs: CostEstimatorInputs,
    models: Iterable[PricingModel] = DEFAULT_PRICING_MODELS,
) -> List[CostEstimate]:
    batch_size = inputs.batch_size if inputs.batch_size > 0 else 1
    request_count = max(1, math.ceil(inputs.company_count / batch_size))
    total_input_tokens = (
        inputs.company_count * inputs.input_tokens_per_company
        + request_count * inputs.shared_input_tokens_per_request
    )
    total_output_tokens = inputs.company_count * inputs.output_tokens_per_company

    estimates = []
    for pricing_model in models:
        estimated_input_cost = (
            total_input_tokens * pricing_model.input_price_per_million_tokens / 1_000_000
        )
        estimated_output_cost = (
            total_output_tokens * pricing_model.output_price_per_million_tokens / 1_000_000
        )
        estimates.append(
            CostEstimate(
                provider=pricing_model.provider,
                model=pricing_model.model,
                currency=pricing_model.currency,
                request_count=request_count,
                total_input_tokens=total_input_tokens,
                total_output_tokens=total_output_tokens,
                estimated_input_cost=estimated_input_cost,
                estimated_output_cost=estimated_output_cost,
                estimated_total_cost=estimated_input_cost + estimated_output_cost,
            )
        )

    return estimates
