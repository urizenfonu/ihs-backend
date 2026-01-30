from models import CompositeRule, EvaluationResult
from rules.parameter_mapper import extract_value
from rules.simple_rules import SimpleRuleEvaluator

class RateChangeEvaluator:
    """Evaluates rules based on rate of change between consecutive readings"""

    def __init__(self, db):
        self.db = db
        self.simple_evaluator = SimpleRuleEvaluator()

    async def evaluate(
        self,
        rule: CompositeRule,
        asset_id: int,
        current_reading: dict
    ) -> EvaluationResult:
        """Detect sudden changes between consecutive readings"""
        if not rule.conditions or len(rule.conditions) == 0:
            return EvaluationResult(
                triggered=False,
                message="No conditions defined",
                reason="No conditions"
            )

        # Get previous reading
        prev_reading = await self.db.get_previous_reading(asset_id)
        if not prev_reading:
            return EvaluationResult(
                triggered=False,
                message="No previous reading available",
                reason="No previous reading"
            )

        condition = rule.conditions[0]
        current_value = extract_value(condition.parameter, current_reading)
        prev_value = extract_value(condition.parameter, prev_reading.get('data', {}))

        if current_value is None or prev_value is None:
            return EvaluationResult(
                triggered=False,
                message=f"Missing data for {condition.parameter}",
                reason="Missing data"
            )

        # Calculate change
        change = abs(current_value - prev_value)
        direction = "increase" if current_value > prev_value else "decrease"

        # Check if change exceeds threshold
        triggered = self.simple_evaluator.compare(change, condition.operator, condition.value)

        message = f"{condition.parameter} {direction} of {change:.2f}{condition.unit}"
        if triggered:
            message = f"ALERT: {message} exceeds threshold {condition.value}{condition.unit}"

        return EvaluationResult(
            triggered=triggered,
            value=change,
            threshold=condition.value,
            rate_of_change=change,
            message=message
        )
