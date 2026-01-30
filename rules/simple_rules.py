from models import CompositeRule, EvaluationResult
from rules.parameter_mapper import extract_value

class SimpleRuleEvaluator:
    """Evaluates single-condition rules"""

    def evaluate(self, rule: CompositeRule, reading: dict) -> EvaluationResult:
        """Evaluate single-condition rule"""
        if not rule.conditions or len(rule.conditions) == 0:
            return EvaluationResult(
                triggered=False,
                message="No conditions defined",
                reason="No conditions"
            )

        condition = rule.conditions[0]
        value = extract_value(condition.parameter, reading)

        if value is None:
            return EvaluationResult(
                triggered=False,
                message=f"No data for {condition.parameter}",
                reason="No data"
            )

        triggered = self.compare(value, condition.operator, condition.value)

        return EvaluationResult(
            triggered=triggered,
            value=value,
            threshold=condition.value,
            message=f"{condition.parameter} is {value} {condition.unit}" if triggered else f"{condition.parameter} check passed"
        )

    def compare(self, value: float, operator: str, threshold: float) -> bool:
        """Compare value against threshold using operator"""
        ops = {
            '<=': lambda v, t: v <= t,
            '<': lambda v, t: v < t,
            '>=': lambda v, t: v >= t,
            '>': lambda v, t: v > t,
            '==': lambda v, t: abs(v - t) < 0.01,  # Floating point equality
            '!=': lambda v, t: abs(v - t) >= 0.01,
        }

        if operator not in ops:
            return False

        return ops[operator](value, threshold)
