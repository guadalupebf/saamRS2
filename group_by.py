from django.db.models import Count, Sum


def polizas_por_aseguradora(polizas):
    prueba = {}
    values1 = ['aseguradora__compania','f_currency']

    polizas_pesos = polizas.filter(f_currency = 1)
    polizas_dolares = polizas.filter(f_currency = 2)
    polizas_udi = polizas.filter(f_currency = 3)
    prueba['pesos_pol'] = polizas_pesos.filter(document_type__in=[1, 3, 4]).values(*values1).annotate(Count('aseguradora')).annotate(Sum('p_neta')).annotate(Sum('p_total')).annotate(Sum('comision')).order_by('aseguradora__alias')
    prueba['pesos_fia'] = polizas_pesos.filter(document_type__in=[7, 8]).values(*values1).annotate(Count('aseguradora')).annotate(Sum('p_neta')).annotate(Sum('p_total')).annotate(Sum('comision')).order_by('aseguradora__alias')
    prueba['dolares_pol'] = polizas_dolares.filter(document_type__in=[1, 3, 4]).values(*values1).annotate(Count('aseguradora')).annotate(Sum('p_neta')).annotate(Sum('p_total')).annotate(Sum('comision')).order_by('aseguradora__alias')
    prueba['dolares_fia'] = polizas_dolares.filter(document_type__in=[7, 8]).values(*values1).annotate(Count('aseguradora')).annotate(Sum('p_neta')).annotate(Sum('p_total')).annotate(Sum('comision')).order_by('aseguradora__alias')
    prueba['udi_pol'] = polizas_udi.filter(document_type__in=[1, 3, 4]).values(*values1).annotate(Count('aseguradora')).annotate(Sum('p_neta')).annotate(Sum('p_total')).annotate(Sum('comision')).order_by('aseguradora__alias')
    prueba['udi_fia'] = polizas_udi.filter(document_type__in=[7, 8]).values(*values1).annotate(Count('aseguradora')).annotate(Sum('p_neta')).annotate(Sum('p_total')).annotate(Sum('comision')).order_by('aseguradora__alias')

    values2 = ['aseguradora__compania','p_neta__sum','p_total__sum','comision__sum','f_currency','aseguradora__count']
    rows_pesos = prueba['pesos_pol'].values_list(*values2)
    rows_pesosf = prueba['pesos_fia'].values_list(*values2)
    rows_dolares = prueba['dolares_pol'].values_list(*values2)
    rows_dolaresf = prueba['dolares_fia'].values_list(*values2)
    rows_udi = prueba['udi_pol'].values_list(*values2)
    rows_udif = prueba['udi_fia'].values_list(*values2)

    return rows_pesos, rows_dolares, rows_pesosf, rows_dolaresf,rows_udi,rows_udif

def polizas_por_ramo(polizas):
    polizas_pesos = polizas.filter(f_currency = 1)
    polizas_dolares = polizas.filter(f_currency = 2)
    # prueba = polizas.values('ramo__ramo_name').annotate(Count('ramo')).annotate(Sum('p_neta')).annotate(Sum('p_total')).annotate(Sum('comision')).order_by('ramo__ramo_name')
    polizas_udi = polizas.filter(f_currency = 3) 
    
    prueba = {}
    prueba['pesos_pol'] = polizas_pesos.filter(document_type__in=[1, 3, 4]).values('ramo__ramo_name','f_currency').annotate(Count('ramo')).annotate(Sum('p_neta')).annotate(Sum('p_total')).annotate(Sum('comision')).order_by('ramo__ramo_name')
    prueba['dolares_pol'] = polizas_dolares.filter(document_type__in=[1, 3, 4]).values('ramo__ramo_name','f_currency').annotate(Count('ramo')).annotate(Sum('p_neta')).annotate(Sum('p_total')).annotate(Sum('comision')).order_by('ramo__ramo_name')

    prueba['pesos_fia'] = polizas_pesos.filter(document_type__in=[7, 8]).values('ramo__ramo_name','f_currency').annotate(Count('ramo')).annotate(Sum('p_neta')).annotate(Sum('p_total')).annotate(Sum('comision')).order_by('ramo__ramo_name')
    prueba['dolares_fia'] = polizas_dolares.filter(document_type__in=[7, 8]).values('ramo__ramo_name','f_currency').annotate(Count('ramo')).annotate(Sum('p_neta')).annotate(Sum('p_total')).annotate(Sum('comision')).order_by('ramo__ramo_name')
    # UDI
    prueba['udi_pol'] = polizas_udi.filter(document_type__in=[1, 3, 4]).values('ramo__ramo_name').annotate(Count('ramo')).annotate(Sum('p_neta')).annotate(Sum('p_total')).annotate(Sum('comision')).order_by('ramo__ramo_name')
    prueba['udi_fia'] = polizas_udi.filter(document_type__in=[7, 8]).values('ramo__ramo_name','f_currency').annotate(Count('ramo')).annotate(Sum('p_neta')).annotate(Sum('p_total')).annotate(Sum('comision')).order_by('ramo__ramo_name')

    values = ['ramo__ramo_name','p_neta__sum','p_total__sum','comision__sum','f_currency','ramo__count']
    rows_pesos = prueba['pesos_pol'].values_list(*values)
    rows_pesosf = prueba['pesos_fia'].values_list(*values)
    rows_dolares = prueba['dolares_pol'].values_list(*values)
    rows_dolaresf = prueba['dolares_fia'].values_list(*values)
    rows_udi = prueba['udi_pol'].values_list(*values)
    rows_udif = prueba['udi_fia'].values_list(*values)

    return rows_pesos, rows_dolares, rows_pesosf, rows_dolaresf,rows_udi,rows_udif


def polizas_por_subramo(polizas):
    polizas_pesos = polizas.filter(f_currency = 1)
    polizas_dolares = polizas.filter(f_currency = 2)
    polizas_udi = polizas.filter(f_currency = 3)
    prueba = {}

    values1 = ['subramo__subramo_name','f_currency']
    prueba['pesos_pol'] = polizas_pesos.exclude(document_type__in=[7, 8, 9, 10]).values(*values1).annotate(Count('subramo')).annotate(Sum('p_neta')).annotate(Sum('p_total')).annotate(Sum('comision')).order_by('subramo__subramo_name')
    prueba['pesos_fia'] = polizas_pesos.filter(document_type__in=[7, 8, 9, 10]).values(*values1).annotate(Count('subramo')).annotate(Sum('p_neta')).annotate(Sum('p_total')).annotate(Sum('comision')).order_by('subramo__subramo_name')
    prueba['dolares_pol'] = polizas_dolares.exclude(document_type__in=[7, 8, 9, 10]).values(*values1).annotate(Count('subramo')).annotate(Sum('p_neta')).annotate(Sum('p_total')).annotate(Sum('comision')).order_by('subramo__subramo_name')
    prueba['dolares_fia'] = polizas_dolares.filter(document_type__in=[7, 8, 9, 10]).values(*values1).annotate(Count('subramo')).annotate(Sum('p_neta')).annotate(Sum('p_total')).annotate(Sum('comision')).order_by('subramo__subramo_name')
    prueba['udi_pol'] = polizas_udi.exclude(document_type__in=[7, 8, 9, 10]).values(*values1).annotate(Count('subramo')).annotate(Sum('p_neta')).annotate(Sum('p_total')).annotate(Sum('comision')).order_by('subramo__subramo_name')
    prueba['udi_fia'] = polizas_udi.filter(document_type__in=[7, 8, 9, 10]).values(*values1).annotate(Count('subramo')).annotate(Sum('p_neta')).annotate(Sum('p_total')).annotate(Sum('comision')).order_by('subramo__subramo_name')

    values2 = ['subramo__subramo_name','p_neta__sum','p_total__sum','comision__sum','f_currency','subramo__count']
    rows_pesos = prueba['pesos_pol'].values_list(*values2)
    rows_pesosf = prueba['pesos_fia'].values_list(*values2)
    rows_dolares = prueba['dolares_pol'].values_list(*values2)
    rows_dolaresf = prueba['dolares_fia'].values_list(*values2)
    rows_udi = prueba['udi_pol'].values_list(*values2)
    rows_udif = prueba['udi_fia'].values_list(*values2)

    return rows_pesos, rows_dolares, rows_pesosf, rows_dolaresf,rows_udi,rows_udif

def polizas_por_grupo(polizas):
    prueba = {}
    polizas_pesos_j = polizas.filter(f_currency = 1, contractor__type_person =2)
    polizas_pesos_n = polizas.filter(f_currency = 1, contractor__type_person =1)
    polizas_dolares_n = polizas.filter(f_currency = 2, contractor__type_person =1)
    polizas_dolares_j = polizas.filter(f_currency = 2, contractor__type_person =2)
    polizas_udi_n = polizas.filter(f_currency = 3, contractor__type_person =1)
    polizas_udi_j = polizas.filter(f_currency = 3, contractor__type_person =2)
    prueba_nat_pesos = polizas_pesos_n.values('contractor__group__group_name','f_currency').annotate(Count('contractor__group__group_name')).annotate(Sum('p_neta')).annotate(Sum('comision')).annotate(Sum('p_total')).order_by('contractor__group__group_name')
    prueba_jur_pesos = polizas_pesos_j.values('contractor__group__group_name','f_currency').annotate(Count('contractor__group__group_name')).annotate(Sum('p_neta')).annotate(Sum('comision')).annotate(Sum('p_total')).order_by('contractor__group__group_name')
    prueba_nat_dolares = polizas_dolares_n.values('contractor__group__group_name','f_currency').annotate(Count('contractor__group__group_name')).annotate(Sum('p_neta')).annotate(Sum('comision')).annotate(Sum('p_total')).order_by('contractor__group__group_name')
    prueba_jur_dolares = polizas_dolares_j.values('contractor__group__group_name','f_currency').annotate(Count('contractor__group__group_name')).annotate(Sum('p_neta')).annotate(Sum('comision')).annotate(Sum('p_total')).order_by('contractor__group__group_name')
    prueba_nat_udi = polizas_udi_n.values('contractor__group__group_name','f_currency').annotate(Count('contractor__group__group_name')).annotate(Sum('p_neta')).annotate(Sum('comision')).annotate(Sum('p_total')).order_by('contractor__group__group_name')
    prueba_jur_udi = polizas_udi_j.values('contractor__group__group_name','f_currency').annotate(Count('contractor__group__group_name')).annotate(Sum('p_neta')).annotate(Sum('comision')).annotate(Sum('p_total')).order_by('contractor__group__group_name')
    
    prueba['naturals_pesos'] = prueba_nat_pesos
    prueba['juridicals_pesos'] = prueba_jur_pesos
    prueba['naturals_dolares'] = prueba_nat_dolares
    prueba['juridicals_dolares'] = prueba_jur_dolares
    prueba['naturals_udi'] = prueba_nat_udi
    prueba['juridicals_udi'] = prueba_jur_udi

    valuesn = ['contractor__group__group_name','p_neta__sum','p_total__sum','comision__sum','f_currency','contractor__group__group_name__count']
    valuesj = ['contractor__group__group_name','p_neta__sum','p_total__sum','comision__sum','f_currency','contractor__group__group_name__count']
    rows_pesos = prueba['naturals_pesos'].values_list(*valuesn)
    rows_pesosf = prueba['juridicals_pesos'].values_list(*valuesj)
    rows_dolares = prueba['naturals_dolares'].values_list(*valuesn)
    rows_dolaresf = prueba['juridicals_dolares'].values_list(*valuesj)
    rows_udi = prueba['naturals_udi'].values_list(*valuesn)
    rows_udif = prueba['juridicals_udi'].values_list(*valuesj)
    return rows_pesos, rows_dolares, rows_pesosf, rows_dolaresf,rows_udi,rows_udif

def polizas_por_referenciadores(polizas):
    prueba = {}
    polizas_pesos = polizas.filter(f_currency = 1)
    polizas_dolares = polizas.filter(f_currency = 2)
    polizas_udi = polizas.filter(f_currency = 3)

    prueba_prefs_pesos = polizas_pesos.values('ref_policy__referenciador__first_name', 'ref_policy__referenciador__last_name').annotate(Count('ref_policy__referenciador'),Count('ref_policy__referenciador__id')).annotate(Sum('p_neta')).annotate(Sum('comision')).annotate(Sum('p_total')).order_by('ref_policy__referenciador__id')
    prueba_prefs_dols = polizas_dolares.values('ref_policy__referenciador__first_name', 'ref_policy__referenciador__last_name').annotate(Count('ref_policy__referenciador'),Count('ref_policy__referenciador__id')).annotate(Sum('p_neta')).annotate(Sum('comision')).annotate(Sum('p_total')).order_by('ref_policy__referenciador__id')
    prueba_prefs_udi = polizas_udi.values('ref_policy__referenciador__first_name', 'ref_policy__referenciador__last_name').annotate(Count('ref_policy__referenciador'),Count('ref_policy__referenciador__id')).annotate(Sum('p_neta')).annotate(Sum('comision')).annotate(Sum('p_total')).order_by('ref_policy__referenciador__id')
    prueba['polizas_pesos'] = prueba_prefs_pesos
    prueba['polizas_dolares'] = prueba_prefs_dols
    prueba['polizas_udi'] = prueba_prefs_udi
    c = prueba['polizas_pesos'].values('ref_policy__referenciador__first_name', 'ref_policy__referenciador__last_name','ref_policy__referenciador__id__count','p_neta__sum','comision__sum','p_total__sum')
    c_ = prueba['polizas_dolares'].values('ref_policy__referenciador__first_name', 'ref_policy__referenciador__last_name','ref_policy__referenciador__id__count','p_neta__sum','comision__sum','p_total__sum')
    uc_ = prueba['polizas_udi'].values('ref_policy__referenciador__first_name', 'ref_policy__referenciador__last_name','ref_policy__referenciador__id__count','p_neta__sum','comision__sum','p_total__sum')
    prueba['polizas_pesos_refs'] = c
    prueba['polizas_pesos'] = c
    prueba['polizas_dolares_refs'] = c_
    prueba['polizas_dolares'] = c_
    prueba['polizas_udi_refs'] = uc_
    prueba['polizas_udi'] = uc_

    prueba_prefs_pesos = polizas_pesos.values('ref_policy__referenciador__first_name', 'ref_policy__referenciador__last_name').annotate(Count('ref_policy__referenciador'),Count('ref_policy__referenciador__id')).annotate(Sum('p_neta')).annotate(Sum('comision')).annotate(Sum('p_total')).order_by('ref_policy__referenciador__id')
    prueba_prefs_dols = polizas_dolares.values('ref_policy__referenciador__first_name', 'ref_policy__referenciador__last_name').annotate(Count('ref_policy__referenciador'),Count('ref_policy__referenciador__id')).annotate(Sum('p_neta')).annotate(Sum('comision')).annotate(Sum('p_total')).order_by('ref_policy__referenciador__id')
    prueba_prefs_udi = polizas_udi.values('ref_policy__referenciador__first_name', 'ref_policy__referenciador__last_name').annotate(Count('ref_policy__referenciador'),Count('ref_policy__referenciador__id')).annotate(Sum('p_neta')).annotate(Sum('comision')).annotate(Sum('p_total')).order_by('ref_policy__referenciador__id')

    prueba['polizas_pesos_r'] = prueba_prefs_pesos
    prueba['polizas_dolares_r'] = prueba_prefs_dols            
    prueba['polizas_udi_r'] = prueba_prefs_udi      
    rows_pesos = prueba['polizas_pesos'].values_list('ref_policy__referenciador__first_name','p_neta__sum','p_total__sum','comision__sum','f_currency','ref_policy__referenciador__id__count','ref_policy__referenciador__last_name')
    rows_dolares = prueba['polizas_dolares'].values_list('ref_policy__referenciador__first_name','p_neta__sum','p_total__sum','comision__sum','f_currency','ref_policy__referenciador__id__count','ref_policy__referenciador__last_name')
    rows_udi = prueba['polizas_dolares'].values_list('ref_policy__referenciador__first_name','p_neta__sum','p_total__sum','comision__sum','f_currency','ref_policy__referenciador__id__count','ref_policy__referenciador__last_name')
    return rows_pesos, rows_dolares,rows_udi

def polizas_por_owner(polizas):
    prueba = {}
    polizas_pesos = polizas.filter(f_currency = 1)
    polizas_dolares = polizas.filter(f_currency = 2)
    polizas_udi = polizas.filter(f_currency = 3)
    prueba_p_pesos = polizas_pesos.filter(document_type__in=[1, 3, 4]).values('owner__first_name', 'owner__last_name','f_currency').annotate(Count('owner')).annotate(Sum('p_neta')).annotate(Sum('comision')).annotate(Sum('p_total'))
    prueba_f_pesos = polizas_pesos.filter(document_type__in=[7, 8]).values('owner__first_name', 'owner__last_name','f_currency').annotate(Count('owner')).annotate(Sum('p_neta')).annotate(Sum('comision')).annotate(Sum('p_total'))
    prueba_p_dolares = polizas_dolares.filter(document_type__in=[1, 3, 4]).values('owner__first_name', 'owner__last_name','f_currency').annotate(Count('owner')).annotate(Sum('p_neta')).annotate(Sum('comision')).annotate(Sum('p_total'))
    prueba_f_dolares = polizas_dolares.filter(document_type__in=[7, 8]).values('owner__first_name', 'owner__last_name','f_currency').annotate(Count('owner')).annotate(Sum('p_neta')).annotate(Sum('comision')).annotate(Sum('p_total'))
    prueba_p_udi = polizas_udi.filter(document_type__in=[1, 3, 4]).values('owner__first_name', 'owner__last_name','f_currency').annotate(Count('owner')).annotate(Sum('p_neta')).annotate(Sum('comision')).annotate(Sum('p_total'))
    prueba_f_udi = polizas_udi.filter(document_type__in=[7, 8]).values('owner__first_name', 'owner__last_name','f_currency').annotate(Count('owner')).annotate(Sum('p_neta')).annotate(Sum('comision')).annotate(Sum('p_total'))
    prueba['polizas_pesos'] = prueba_p_pesos
    prueba['fianzas_pesos'] = prueba_f_pesos
    prueba['polizas_dolares'] = prueba_p_dolares
    prueba['fianzas_dolares'] = prueba_f_dolares
    prueba['polizas_udi'] = prueba_p_udi
    prueba['fianzas_udi'] = prueba_f_udi
    rows_pesos = prueba['polizas_pesos'].values_list('owner__first_name','p_neta__sum','p_total__sum','comision__sum','f_currency','owner__count','owner__last_name')
    rows_pesosf = prueba['fianzas_pesos'].values_list('owner__first_name','p_neta__sum','p_total__sum','comision__sum','f_currency','owner__count','owner__last_name')
    rows_dolares = prueba['polizas_dolares'].values_list('owner__first_name','p_neta__sum','p_total__sum','comision__sum','f_currency','owner__count','owner__last_name')
    rows_dolaresf = prueba['fianzas_dolares'].values_list('owner__first_name','p_neta__sum','p_total__sum','comision__sum','f_currency','owner__count','owner__last_name')
    rows_udi = prueba['polizas_udi'].values_list('owner__first_name','p_neta__sum','p_total__sum','comision__sum','f_currency','owner__count','owner__last_name')
    rows_udif = prueba['fianzas_udi'].values_list('owner__first_name','p_neta__sum','p_total__sum','comision__sum','f_currency','owner__count','owner__last_name')
    return rows_pesos, rows_dolares, rows_pesosf, rows_dolaresf,rows_udi,rows_udif

def polizas_por_estatus(polizas):
    polizas_pesos = polizas.filter(f_currency = 1)
    polizas_udi = polizas.filter(f_currency = 3)
    polizas_dolares = polizas.filter(f_currency = 2)
    prueba = {}

    values1 = ['status','f_currency']
    prueba['pesos_pol'] = polizas_pesos.exclude(document_type__in=[7, 8, 9, 10]).values(*values1).annotate(Count('subramo')).annotate(Sum('p_neta')).annotate(Sum('p_total')).annotate(Sum('comision')).order_by('status')
    prueba['pesos_fia'] = polizas_pesos.filter(document_type__in=[7, 8, 9, 10]).values(*values1).annotate(Count('subramo')).annotate(Sum('p_neta')).annotate(Sum('p_total')).annotate(Sum('comision')).order_by('status')
    prueba['dolares_pol'] = polizas_dolares.exclude(document_type__in=[7, 8, 9, 10]).values(*values1).annotate(Count('subramo')).annotate(Sum('p_neta')).annotate(Sum('p_total')).annotate(Sum('comision')).order_by('status')
    prueba['dolares_fia'] = polizas_dolares.filter(document_type__in=[7, 8, 9, 10]).values(*values1).annotate(Count('subramo')).annotate(Sum('p_neta')).annotate(Sum('p_total')).annotate(Sum('comision')).order_by('status')
    prueba['udi_pol'] = polizas_udi.exclude(document_type__in=[7, 8, 9, 10]).values(*values1).annotate(Count('subramo')).annotate(Sum('p_neta')).annotate(Sum('p_total')).annotate(Sum('comision')).order_by('status')
    prueba['udi_fia'] = polizas_udi.filter(document_type__in=[7, 8, 9, 10]).values(*values1).annotate(Count('subramo')).annotate(Sum('p_neta')).annotate(Sum('p_total')).annotate(Sum('comision')).order_by('status')

    values2 = ['status','p_neta__sum','p_total__sum','comision__sum','f_currency','subramo__count']
    rows_pesos = prueba['pesos_pol'].values_list(*values2)
    rows_dolares = prueba['dolares_pol'].values_list(*values2)
    rows_udi = prueba['udi_pol'].values_list(*values2)
    rows_pesosf = prueba['pesos_fia'].values_list(*values2)
    rows_dolaresf = prueba['dolares_fia'].values_list(*values2)

    return rows_pesos, rows_dolares, rows_udi


def polizas_por_contratante(polizas):
    prueba = {}
    polizas_pesos_j = polizas.filter(f_currency = 1, contractor__type_person =2)
    polizas_pesos_n = polizas.filter(f_currency = 1, contractor__type_person =1)
    polizas_dolares_n = polizas.filter(f_currency = 2, contractor__type_person =1)
    polizas_dolares_j = polizas.filter(f_currency = 2, contractor__type_person =2)
    polizas_udi_n = polizas.filter(f_currency = 3, contractor__type_person =1)
    polizas_udi_j = polizas.filter(f_currency = 3, contractor__type_person =2)
    prueba_nat_pesos = polizas_pesos_n.values('contractor__full_name','f_currency').annotate(Count('contractor__full_name')).annotate(Sum('p_neta')).annotate(Sum('comision')).annotate(Sum('p_total')).order_by('contractor__full_name')
    prueba_jur_pesos = polizas_pesos_j.values('contractor__full_name','f_currency').annotate(Count('contractor__full_name')).annotate(Sum('p_neta')).annotate(Sum('comision')).annotate(Sum('p_total')).order_by('contractor__full_name')
    prueba_nat_dolares = polizas_dolares_n.values('contractor__full_name','f_currency').annotate(Count('contractor__full_name')).annotate(Sum('p_neta')).annotate(Sum('comision')).annotate(Sum('p_total')).order_by('contractor__full_name')
    prueba_jur_dolares = polizas_dolares_j.values('contractor__full_name','f_currency').annotate(Count('contractor__full_name')).annotate(Sum('p_neta')).annotate(Sum('comision')).annotate(Sum('p_total')).order_by('contractor__full_name')
    prueba_nat_udi = polizas_udi_n.values('contractor__full_name','f_currency').annotate(Count('contractor__full_name')).annotate(Sum('p_neta')).annotate(Sum('comision')).annotate(Sum('p_total')).order_by('contractor__full_name')
    prueba_jur_udi = polizas_udi_j.values('contractor__full_name','f_currency').annotate(Count('contractor__full_name')).annotate(Sum('p_neta')).annotate(Sum('comision')).annotate(Sum('p_total')).order_by('contractor__full_name')
    
    prueba['naturals_pesos'] = prueba_nat_pesos
    prueba['juridicals_pesos'] = prueba_jur_pesos
    prueba['naturals_dolares'] = prueba_nat_dolares
    prueba['juridicals_dolares'] = prueba_jur_dolares
    prueba['naturals_udi'] = prueba_nat_udi
    prueba['juridicals_udi'] = prueba_jur_udi

    valuesn = ['contractor__full_name','p_neta__sum','p_total__sum','comision__sum','f_currency','contractor__full_name__count']
    valuesj = ['contractor__full_name','p_neta__sum','p_total__sum','comision__sum','f_currency','contractor__full_name__count']
    rows_pesos = prueba['naturals_pesos'].values_list(*valuesn)
    rows_pesosf = prueba['juridicals_pesos'].values_list(*valuesj)
    rows_dolares = prueba['naturals_dolares'].values_list(*valuesn)
    rows_dolaresf = prueba['juridicals_dolares'].values_list(*valuesj)
    rows_udi = prueba['naturals_udi'].values_list(*valuesn)
    rows_udif = prueba['juridicals_udi'].values_list(*valuesj)
    return rows_pesos, rows_dolares, rows_pesosf, rows_dolaresf,rows_udi,rows_udif