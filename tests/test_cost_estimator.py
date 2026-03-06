from company_gtm_enricher.cost_estimator import (
    DEFAULT_PRICING_MODELS,
    CostEstimatorInputs,
    estimate_costs,
)


def test_estimate_costs_calculates_expected_totals() -> None:
    estimates = estimate_costs(
        CostEstimatorInputs(
            company_count=800,
            batch_size=10,
            shared_input_tokens_per_request=900,
            input_tokens_per_company=90,
            output_tokens_per_company=180,
        )
    )

    first_estimate = estimates[0]
    assert first_estimate.request_count == 80
    assert first_estimate.total_input_tokens == 144000
    assert first_estimate.total_output_tokens == 144000


def test_default_pricing_models_cover_three_providers_with_two_models_each() -> None:
    providers = {}
    for pricing_model in DEFAULT_PRICING_MODELS:
        providers.setdefault(pricing_model.provider, []).append(pricing_model.model)

    assert set(providers) == {"OpenAI", "Anthropic", "MiniMax"}
    assert all(len(models) == 2 for models in providers.values())
