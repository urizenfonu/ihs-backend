from models import CompositeRule, EvaluationResult
from rules.parameter_mapper import extract_value
from rules.simple_rules import SimpleRuleEvaluator

class CompositeRuleEvaluator:
    """Evaluates multi-condition rules with AND/OR logic"""

    def __init__(self):
        self.simple_evaluator = SimpleRuleEvaluator()

    async def evaluate(self, rule: CompositeRule, reading: dict) -> EvaluationResult:
        """Evaluate multi-condition rule with AND/OR logic"""
        if not rule.conditions or len(rule.conditions) == 0:
            return EvaluationResult(
                triggered=False,
                message="No conditions defined",
                reason="No conditions"
            )

        results = []
        values = []

        for condition in rule.conditions:
            value = extract_value(condition.parameter, reading)
            if value is None:
                results.append(False)
                values.append(None)
                continue

            result = self.simple_evaluator.compare(value, condition.operator, condition.value)
            results.append(result)
            values.append(value)

        # Apply logical operator
        if rule.logical_operator == 'AND':
            triggered = all(results)
        elif rule.logical_operator == 'OR':
            triggered = any(results)
        else:
            # Default to AND if not specified
            triggered = all(results)

        conditions_met = sum(results)
        total_conditions = len(results)

        # Build message
        if triggered:
            message = f"{rule.name}: {conditions_met}/{total_conditions} conditions met"
        else:
            message = f"{rule.name}: Only {conditions_met}/{total_conditions} conditions met"

        return EvaluationResult(
            triggered=triggered,
            conditions_met=conditions_met,
            total_conditions=total_conditions,
            message=message
        )
