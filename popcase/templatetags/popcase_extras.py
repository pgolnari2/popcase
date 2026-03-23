from django import template

register = template.Library()

@register.filter
def get_item(d, k):
    """Safely get dict item in Django templates: {{ mydict|get_item:key }}"""
    if isinstance(d, dict):
        return d.get(k)
    return None