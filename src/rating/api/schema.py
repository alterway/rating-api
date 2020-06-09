
class ValidationError(Exception):
    pass


class Rules:

    def validate_rule_entry(self, metric=None, price=None, unit=None):
        if not isinstance(metric, str) or \
           not isinstance(price, (float, int)) or \
           not isinstance(unit, str):
            raise ValidationError('Wrong parameter for Rule entry')
        return {
            'metric': metric,
            'price': price,
            'unit': unit
        }

    def validate_rule_config(self, label_set={}, ruleset=None):
        if not isinstance(label_set, dict) or \
           not isinstance(ruleset, list):
            raise ValidationError('Wrong parameter type for ruleset or labelSet')
        elif len(ruleset) < 1:
            raise ValidationError('No configuration provided for ruleset config')
        return {
            'labelSet': label_set,
            'ruleset': [
                self.validate_rule_entry(
                    metric=rule.get('metric'),
                    price=rule.get('price'),
                    unit=rule.get('unit')
                )
                for rule in ruleset]
        }

    def validate_rules(self, rules=None):
        if not isinstance(rules, list):
            raise ValidationError(
                f'Wrong type for rules, expected list got {type(rules)}')
        elif len(rules) < 1:
            raise ValidationError('No configuration provided for rules')
        validated_rules = []
        for rule in rules:
            validated_rules.append(
                self.validate_rule_config(
                    label_set=rule.get('labelSet', {}),
                    ruleset=rule.get('ruleset')
                )
            )
        return validated_rules

    def __init__(self, content):
        self.valid_ruleset = self.validate_rules(rules=content.get('rules'))


class Metrics:

    def validate_metric_config(self,
                               presto_column=None,
                               presto_table=None,
                               report_name=None,
                               unit=None):
        if not isinstance(presto_column, str) or \
           not isinstance(presto_table, str) or \
           not isinstance(report_name, str) or \
           not isinstance(unit, str):
            raise ValidationError(
                f'Wrong parameter for metric config, got\
                [{presto_column},{presto_table},{report_name},{unit}]')
        return {
            'presto_column': presto_column,
            'presto_table': presto_table,
            'report_name': report_name,
            'unit': unit
        }

    def validate_metric(self, metrics=None):
        if not isinstance(metrics, dict):
            raise ValidationError(
                f'Wrong type for metrics, expected dict got {type(metrics)}')
        elif len(metrics) < 1:
            raise ValidationError('No configuration provided for metrics')
        metrics_config = {}
        for name, metric in metrics.items():
            metrics_config.update({
                name: self.validate_metric_config(
                    presto_column=metric.get('presto_column'),
                    presto_table=metric.get('presto_table'),
                    report_name=metric.get('report_name'),
                    unit=metric.get('unit')
                )
            })
        return metrics_config

    def __init__(self, content):
        self.valid_metrics = self.validate_metric(metrics=content.get('metrics'))


def validate_request_content(content):
    try:
        Metrics(content)
        Rules(content)
    except ValidationError as exc:
        raise exc
