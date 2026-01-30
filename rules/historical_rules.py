from models import CompositeRule, EvaluationResult
from rules.parameter_mapper import extract_value
from rules.simple_rules import SimpleRuleEvaluator
from typing import List

class HistoricalRuleEvaluator:
    """Evaluates rules requiring historical data aggregation"""

    def __init__(self, db):
        self.db = db
        self.simple_evaluator = SimpleRuleEvaluator()

    async def evaluate(self, rule: CompositeRule, asset_id: int) -> EvaluationResult:
        """Evaluate rule requiring historical data"""
        if not rule.conditions or len(rule.conditions) == 0:
            return EvaluationResult(
                triggered=False,
                message="No conditions defined",
                reason="No conditions"
            )

        # Get readings for time window
        minutes = rule.time_window_minutes or 4320  # Default 3 days
        readings = await self.db.get_readings_window(asset_id, minutes)

        if len(readings) < 10:  # Need minimum data points
            return EvaluationResult(
                triggered=False,
                message=f"Insufficient data: {len(readings)} readings",
                reason="Insufficient data",
                samples=len(readings)
            )

        # Calculate aggregate
        condition = rule.conditions[0]
        aggregate_value = self.calculate_aggregate(
            readings,
            condition.parameter,
            rule.aggregation_type or 'avg'
        )

        if aggregate_value is None:
            return EvaluationResult(
                triggered=False,
                message=f"No valid data for {condition.parameter}",
                reason="No valid data",
                samples=len(readings)
            )

        # Compare against threshold
        triggered = self.simple_evaluator.compare(
            aggregate_value,
            condition.operator,
            condition.value
        )

        agg_type = rule.aggregation_type or 'avg'
        message = f"{agg_type.upper()} {condition.parameter} over {minutes//60}h: {aggregate_value:.2f}{condition.unit}"

        return EvaluationResult(
            triggered=triggered,
            value=aggregate_value,
            threshold=condition.value,
            samples=len(readings),
            message=message
        )

    def calculate_aggregate(
        self,
        readings: List[dict],
        parameter: str,
        agg_type: str
    ) -> float | None:
        """Calculate aggregate value from readings"""
        values = []

        for reading in readings:
            value = extract_value(parameter, reading.get('data', {}))
            if value is not None:
                values.append(value)

        if not values:
            return None

        if agg_type == 'avg':
            return sum(values) / len(values)
        elif agg_type == 'sum':
            return sum(values)
        elif agg_type == 'min':
            return min(values)
        elif agg_type == 'max':
            return max(values)
        else:
            return sum(values) / len(values)  # Default to avg
