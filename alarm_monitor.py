from database import Database
from models import CompositeRule, Alarm, EvaluationResult
from rules.simple_rules import SimpleRuleEvaluator
from rules.composite_rules import CompositeRuleEvaluator
from rules.historical_rules import HistoricalRuleEvaluator
from rules.rate_change_rules import RateChangeEvaluator
from datetime import datetime
from typing import List
import uuid

class AlarmMonitor:
    """Core alarm evaluation engine"""

    def __init__(self, db_path: str):
        self.db = Database(db_path)
        self.simple_evaluator = SimpleRuleEvaluator()
        self.composite_evaluator = CompositeRuleEvaluator()
        self.historical_evaluator = HistoricalRuleEvaluator(self.db)
        self.rate_change_evaluator = RateChangeEvaluator(self.db)

    async def init(self):
        """Initialize database schema"""
        await self.db.init_schema()

    async def evaluate_all(
        self,
        asset_id: int,
        reading: dict,
        site: str = "Unknown",
        region: str = "Unknown"
    ) -> List[Alarm]:
        """Evaluate all rules for a single reading"""
        alarms = []

        # Load applicable rules
        rules = await self.db.get_rules_for_asset(asset_id)

        for rule in rules:
            try:
                result = await self.evaluate_rule(rule, asset_id, reading)

                if result.triggered:
                    is_duplicate = await self.db.has_active_composite_alarm(
                        asset_id=asset_id,
                        composite_rule_id=rule.id,
                        severity=rule.severity
                    )
                    if is_duplicate:
                        continue
                    alarm = self.create_alarm(rule, result, asset_id, site, region)
                    alarms.append(alarm)
            except Exception as e:
                print(f"Error evaluating rule {rule.id}: {str(e)}")
                continue

        return alarms

    async def evaluate_rule(
        self,
        rule: CompositeRule,
        asset_id: int,
        reading: dict
    ) -> EvaluationResult:
        """Route to appropriate evaluator based on rule type"""
        if rule.rule_type == 'simple':
            return self.simple_evaluator.evaluate(rule, reading)

        elif rule.rule_type == 'composite':
            return await self.composite_evaluator.evaluate(rule, reading)

        elif rule.rule_type == 'historical':
            return await self.historical_evaluator.evaluate(rule, asset_id)

        elif rule.rule_type == 'rate_change':
            return await self.rate_change_evaluator.evaluate(rule, asset_id, reading)

        else:
            return EvaluationResult(
                triggered=False,
                message=f"Unknown rule type: {rule.rule_type}",
                reason="Unknown rule type"
            )

    def create_alarm(
        self,
        rule: CompositeRule,
        result: EvaluationResult,
        asset_id: int,
        site: str,
        region: str
    ) -> Alarm:
        """Create alarm from evaluation result"""
        return Alarm(
            id=str(uuid.uuid4()),
            timestamp=datetime.now(),
            site=site,
            region=region,
            severity=rule.severity,
            category=rule.category,
            message=result.message,
            status='active',
            composite_rule_id=rule.id,
            asset_id=asset_id,
            conditions_met=result.conditions_met,
            total_conditions=result.total_conditions,
            samples=result.samples,
            rate_of_change=result.rate_of_change
        )

    async def get_alarms(
        self,
        status: str = "active",
        severity: str = None,
        site: str = None
    ) -> List[dict]:
        """Get alarms with filters"""
        return await self.db.get_alarms(status=status, severity=severity, site=site)

    async def get_rules(self, category: str = None) -> List[CompositeRule]:
        """Get rules"""
        return await self.db.get_rules(category=category)

    async def count_rules(self) -> int:
        """Count total rules"""
        return await self.db.count_rules()
