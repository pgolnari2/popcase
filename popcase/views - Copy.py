# from __future__ import annotations
#
# from typing import Dict, Any
# from django.contrib.auth.decorators import login_required
# from django.shortcuts import render, redirect
# from django.urls import reverse
# from django.contrib.auth import views as auth_views
# from django.views.decorators.http import require_http_methods
#
# from .forms import GeographicLevelForm, FiltersForm, MeasuresForm, StratificationForm
# from popcase.services import get_case_counts_by_county
#
# STEPS = ["geographic-level", "filters", "measures", "stratification"]
#
# def _session_get(request, key: str, default=None):
#     return request.session.get("popcase_wizard", {}).get(key, default)
#
# def _session_set(request, key: str, value):
#     wizard = request.session.get("popcase_wizard", {})
#     wizard[key] = value
#     request.session["popcase_wizard"] = wizard
#     request.session.modified = True
#
# def _wizard_context(request, current_step: str) -> Dict[str, Any]:
#     geo = _session_get(request, "geographic_level", "none")
#     is_geo_strat = geo not in ("none", "patient")
#     is_patient_level = geo == "patient"
#
#     filters_state = _session_get(request, "filters", {}) or {}
#     cancer_types = filters_state.get("cancer_types") or []
#     prostate_selected = "prostate" in cancer_types
#
#     measures_state = _session_get(request, "measures", {}) or {}
#     disease_selected = measures_state.get("disease_measures") or []
#     gleason_selected = "gleason" in disease_selected
#
#     return {
#         "steps": STEPS,
#         "current_step": current_step,
#         "geographic_level": geo,
#         "is_geo_strat": is_geo_strat,
#         "is_patient_level": is_patient_level,
#         "prostate_selected": prostate_selected,
#         "gleason_selected": gleason_selected,
#     }
#
# class PopcaseLoginView(auth_views.LoginView):
#     template_name = "popcase/login.html"
#
# class PopcaseLogoutView(auth_views.LogoutView):
#     pass
#
# def home(request):
#     if request.user.is_authenticated:
#         return redirect("popcase:wizard_step", step="geographic-level")
#     return redirect("popcase:login")
#
# @login_required
# @require_http_methods(["GET", "POST"])
# def wizard_step(request, step: str = "geographic-level"):
#     if step not in STEPS:
#         return redirect("popcase:wizard_step", step="geographic-level")
#
#     form_map = {
#         "geographic-level": GeographicLevelForm,
#         "filters": FiltersForm,
#         "measures": MeasuresForm,
#         "stratification": StratificationForm,
#     }
#     tmpl_map = {
#         "geographic-level": "popcase/wizard/geographic_level.html",
#         "filters": "popcase/wizard/filters.html",
#         "measures": "popcase/wizard/measures.html",
#         "stratification": "popcase/wizard/stratification.html",
#     }
#
#     FormClass = form_map[step]
#     initial = _session_get(request, step, {})
#
#     if request.method == "POST":
#         form = FormClass(request.POST)
#         if form.is_valid():
#             _session_set(request, step, form.cleaned_data)
#             # convenience: also store key vars for cross-step logic
#             if step == "geographic-level":
#                 _session_set(request, "geographic_level", form.cleaned_data.get("geographic_level", "none"))
#             # Next / Previous
#             if "prev" in request.POST:
#                 prev_step = STEPS[max(0, STEPS.index(step) - 1)]
#                 return redirect("popcase:wizard_step", step=prev_step)
#             if step != STEPS[-1]:
#                 next_step = STEPS[STEPS.index(step) + 1]
#                 return redirect("popcase:wizard_step", step=next_step)
#             return redirect("popcase:results")
#     else:
#         form = FormClass(initial=initial)
#
#     ctx = _wizard_context(request, step)
#     ctx.update({"form": form})
#     return render(request, tmpl_map[step], ctx)
#
# @login_required
# def results(request):
#     # Placeholder results page: in PopCASE this is where you'd run the query and render maps/tables.
#     wizard = request.session.get("popcase_wizard", {})
#     ctx = _wizard_context(request, "results")
#     ctx["wizard_state"] = wizard
#     return render(request, "popcase/results.html", ctx)
#
# @login_required
# def reset_wizard(request):
#     request.session.pop("popcase_wizard", None)
#     request.session.modified = True
#     return redirect("popcase:wizard_step", step="geographic-level")

from django.shortcuts import render
from popcase.forms import MeasuresSelectionForm
from popcase.services import (
    get_incidence_by_county,
    get_incidence_by_tract,
    get_incidence_by_zcta,
    get_percent_advanced_by_geography,
)


def measures_view(request):
    """
    Measures page with year + geography selectors.
    """

    # Defaults (first load)
    year = "2023"
    geographic_level = "county"

    form = MeasuresSelectionForm(request.GET or None)

    if form.is_valid():
        year = form.cleaned_data["year"]
        geographic_level = form.cleaned_data["geographic_level"]

    # For now, we only support county-level incidence
    incidence_results = []

    if geographic_level == "county":
        incidence_results = get_incidence_by_county(year)
    elif geographic_level == "tract":
        incidence_results = get_incidence_by_tract(year)
    elif geographic_level == "zcta":
        incidence_results = get_incidence_by_zcta(year)

    advanced_results = get_percent_advanced_by_geography(
        year=year,
        geographic_level=geographic_level,
    )

    context = {
        "form": form,
        "year": year,
        "geographic_level": geographic_level,
        "incidence_results": incidence_results,
        "prostate_selected": False,
        "advanced_results": advanced_results,
    }

    return render(request, "popcase/wizard/measures.html", context)
