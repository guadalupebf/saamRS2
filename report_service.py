        total_ids = set()
        for queryset in (pendingRed, pendingGreen, pendingOrange, pendingYellow):
            total_ids.update(queryset.values_list('pk', flat=True))
        total_ = siniestros.filter(pk__in = list(total_ids)) if total_ids else siniestros.none()
        total_ids = set()
        for queryset in (pendingRed, pendingGreen, pendingOrange, pendingYellow):
            total_ids.update(queryset.values_list('pk', flat=True))
        total_ = siniestros.filter(pk__in = list(total_ids)) if total_ids else siniestros.none()
    sinisters = sinisters.filter(poliza__in = list(polizas), org_name=request.GET.get('org')).distinct()
