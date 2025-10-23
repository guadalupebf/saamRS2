from celery import shared_task
from recibos.models import Recibos
from report_service_2.celery import app
import pandas as pd
from time import gmtime, strftime
from vendedores.models import Vendedor
from .utils import *
from core.models import *
from core.utils import *
from forms.models import *
import requests, time
from django.conf import settings
from django.http import HttpResponse, JsonResponse
from django.db.models import Sum, Count
from operator import and_, or_
from functools import reduce
from generics.models import Relationship, Beneficiaries, Personal_Information
from endosos.models import EndorsementCert
from archivos.models import PolizasFile, RecibosFile, EndorsementFile
from endosos.models import Endorsement
from recibos.tasks import send_log
from siniestros.models import Siniestros, Autos, Accidentes
import redis
import numpy as np
from django.conf import settings
from core.models import Notifications
from polizas.group_by import polizas_por_aseguradora, polizas_por_ramo, polizas_por_subramo, polizas_por_grupo,polizas_por_estatus, polizas_por_referenciadores, polizas_por_owner,polizas_por_contratante
from polizas.reports import (
    crea_reporte_agrupado_1p,
    crea_reporte_agrupado_2p,
    crea_reporte_agrupado_3p,
    crea_reporte_agrupado_4p,
    crea_reporte_agrupado_5p,
)
# from polizas.pyexcelerate_prueba import getPolizasCertificados
from datetime import datetime, timedelta
from .models import *
from django.utils import timezone
from decimal import Decimal, InvalidOperation
from typing import Dict, Iterable


def _to_decimal(value, default=Decimal("0")):
    """Return a Decimal representation for aggregation results."""
    if isinstance(value, Decimal):
        return value
    if value in (None, "", False):
        return default
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return default


def _aggregate_currency_totals(polizas_queryset):
    aggregates = polizas_queryset.aggregate(
        total=Sum("p_total"),
        neta=Sum("p_neta"),
        comision=Sum("comision"),
    )
    return {key: _to_decimal(value) for key, value in aggregates.items()}


def _build_currency_summary(polizas_queryset):
    return {
        "pesos": _aggregate_currency_totals(polizas_queryset.filter(f_currency=1)),
        "dolares": _aggregate_currency_totals(polizas_queryset.filter(f_currency=2)),
        "udi": _aggregate_currency_totals(polizas_queryset.filter(f_currency=3)),
    }


def _empty_currency_summary():
    zero_values = {"total": Decimal("0"), "neta": Decimal("0"), "comision": Decimal("0")}
    return {key: zero_values.copy() for key in ("pesos", "dolares", "udi")}


def _apply_exchange_rate(values: Dict[str, Decimal], keys: Iterable[str], rate):
    rate = _to_decimal(rate)
    if not rate:
        return
    for key in keys:
        values[key] = _to_decimal(values.get(key)) * rate

#@shared_task(bind=True)
@app.task(bind=True, queue=settings.QUEUE)
def create_report_polizas_task(self, data, es_asincrono):
    polizas, polizasNewAdd1, polizasNewAdd2 = filter_polizas(data)
    info_org = data['org_info']
    since = data['since']
    until = data['until']
    request_user_id = data['request_user_id']
    user = User.objects.get(id=request_user_id)


    provider = data['provider']
    ramo = data['ramo']
    subramo = data['subramo'] 
    report_by = data['report_by']   
    since = data['since']
    until = data['until']
    status = data['status']
    payment = data['payment']
    contratante = data['contratante']
    group = data['grupo']
    cve = data['cve']
    type_person = data['type_contractor']
    ot_rep = data['ot']
    order = int(data['order'])
    sucursal = int(data['sucursal'])
    asc = int(data['asc'])
    ramos_sel = (data['ramos'])
    subramos_sel = (data['subramos'])
    providers_sel = (data['providers'])
    users_sel = (data['users'])
    identifier = data['identifier']
    poliza_n = data['poliza']
    excel_type = int(data['excel_type'])
    group_by = int(data['group_by'])
    add_renovadas = int(data['renewals'])
    valDolar = data['valDolar']
    valUdi = data['valUdi']
    # --------****
    subgrupo = data['subgrupo']
    subsubgrupo = data['subsubgrupo']
    nivelagrupacion = data['groupinglevel']
    subnivel = data['subgrupinglevel']
    subsubnivel = data['subsubgrupinglevel']
    bsLine = data['businessLine']
    clasificacion = data['classification']

    try:
        origen = data['origin']
    except Exception as eee:
        origen = 0

    if len(info_org['logo']) != 0:
            archivo_imagen = 'https://miurabox.s3.amazonaws.com/cas/' + info_org['logo']
    else:
            archivo_imagen = 'saam.jpg'
    # print(info_org)
    tipo_reporte = "Reporte de polizas del " + since + " al " + until

    row_num = 10 # A partir de aqui pinta la tabla de resultados

    # Permiso de ver comisión
    model = ModelsPermissions.objects.filter(user = request_user_id).values_list('pk', flat = True)
    permiso = UserPermissions.objects.filter(model__in = list(model))
    view_comision = permiso.get(permission_name = 'Comisiones')
    si_com = view_comision.checked
    try:
        currency_summary = _build_currency_summary(polizas)
    except Exception as e:
        print(e)
        currency_summary = _empty_currency_summary()

    sumas = {
        'p_total_total_pesos': currency_summary['pesos']['total'],
        'p_neta_total_pesos': currency_summary['pesos']['neta'],
        'comision_total_pesos': currency_summary['pesos']['comision'],
        'p_total_total_dolares': currency_summary['dolares']['total'],
        'p_neta_total_dolares': currency_summary['dolares']['neta'],
        'comision_total_dolares': currency_summary['dolares']['comision'],
        'p_total_total_udi': currency_summary['udi']['total'],
        'p_neta_total_udi': currency_summary['udi']['neta'],
        'comision_total_udi': currency_summary['udi']['comision'],
    }

    _apply_exchange_rate(
        sumas,
        ('p_total_total_dolares', 'p_neta_total_dolares', 'comision_total_dolares'),
        valDolar,
    )
    _apply_exchange_rate(
        sumas,
        ('p_total_total_udi', 'p_neta_total_udi', 'comision_total_udi'),
        valUdi,
    )
    # Estilo General
    
    # Excel agrupado
    if excel_type == 1:
        # Excel Servicio·····································································3
        prueba = {}
        pesos_poliza = polizas.filter(Q(f_currency = 1))
        dolares_poliza = polizas.filter(Q(f_currency = 2))
        rows_pesos = []
        if group_by == 4:
            columns = ['Agrupación', 'Tipo Contratante','Prima Neta', 'Prima Total', 'Comisión', 'Registros','Moneda']
        else:
            columns = ['Agrupación', 'Prima Neta', 'Prima Total', 'Comisión', 'Registros','Moneda']

        if group_by == 1 or group_by == 2 or group_by == 3:
            if group_by == 1:
                prueba['pesos'] = pesos_poliza.values('aseguradora__compania').annotate(Count('aseguradora__id')).annotate(Sum('p_neta')).annotate(Sum('p_total')).annotate(Sum('comision')).order_by('aseguradora__id')
                prueba['dolares'] = dolares_poliza.values('aseguradora__compania').annotate(Count('aseguradora__id')).annotate(Sum('p_neta')).annotate(Sum('p_total')).annotate(Sum('comision')).order_by('aseguradora__id')
                tipo_reporte = tipo_reporte + " por Aseguradora"
                rows_pesos = prueba['pesos'].values_list('aseguradora__compania','p_neta__sum', 'p_total__sum','comision__sum', 'aseguradora__id__count')
                rows_dolares = prueba['dolares'].values_list('aseguradora__compania','p_neta__sum', 'p_total__sum','comision__sum', 'aseguradora__id__count')
            elif group_by == 2:
                prueba['pesos'] = pesos_poliza.values('ramo__ramo_name').annotate(Count('ramo')).annotate(Sum('p_neta')).annotate(Sum('p_total')).annotate(Sum('comision')).order_by('ramo__ramo_name')
                prueba['dolares'] = dolares_poliza.values('ramo__ramo_name').annotate(Count('ramo')).annotate(Sum('p_neta')).annotate(Sum('p_total')).annotate(Sum('comision')).order_by('ramo__ramo_name')
                tipo_reporte = tipo_reporte + " por Ramo"
                rows_pesos = prueba['pesos'].values_list('ramo__ramo_name','p_neta__sum', 'p_total__sum','comision__sum', 'ramo__count')
                rows_dolares = prueba['dolares'].values_list('ramo__ramo_name','p_neta__sum', 'p_total__sum','comision__sum', 'ramo__count')
                rows_2 = []      
            elif group_by == 3:
                prueba['pesos'] = pesos_poliza.values('subramo__subramo_name').annotate(Count('subramo')).annotate(Sum('p_neta')).annotate(Sum('p_total')).annotate(Sum('comision')).order_by('subramo__subramo_name')
                prueba['dolares'] = dolares_poliza.values('subramo__subramo_name').annotate(Count('subramo')).annotate(Sum('p_neta')).annotate(Sum('p_total')).annotate(Sum('comision')).order_by('subramo__subramo_name')
                tipo_reporte = tipo_reporte +  " por Subramo"
                rows_pesos = prueba['pesos'].values_list('subramo__subramo_name','p_neta__sum', 'p_total__sum', 'comision__sum','subramo__count')
                rows_dolares = prueba['dolares'].values_list('subramo__subramo_name','p_neta__sum', 'p_total__sum', 'comision__sum','subramo__count')
                rows_2 = []                
            agrupacion = []
            prima = []
            total = []
            comision = []
            moneda = []
            registrosA = []
            com_conciliada = []
            dolares = 0
            p = 0
            c_pesos = 0
            cc_pesos = 0
            c_dolares = 0
            cc_dolares = 0
            neta_pesos = 0
            neta_dolares = 0
            for row in rows_pesos:
                if row[0] != None:
                    row_num += 1
                    c_pesos = c_pesos + row[3]
                    cc_pesos = cc_pesos 
                    neta_pesos = neta_pesos + row[1]
                    if row[2]:
                        p = p + row[2]
                    ag = ((((((row[0]).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')    
                    prn = row[1]
                    tot = row[2]
                    com = row[3]
                    mon = 'Pesos'
                    regs = row[4]
                    # cc = row[5]
                    agrupacion.append(ag)
                    prima.append(prn)
                    total.append(tot)
                    comision.append(com)
                    moneda.append(mon)
                    registrosA.append(regs)
                    # com_conciliada.append(cc)
            # dolares 1
            for row_2 in rows_dolares:
                if row_2[0] != None:
                    row_num += 1
                    c_dolares = c_dolares + row_2[3]
                    cc_dolares
                    neta_dolares = neta_dolares + row_2[1]
                    if row_2[2]:
                        dolares = dolares + row_2[2]
                    ag = ((((((row_2[0]).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')   
                    prn = row_2[1]
                    tot = row_2[2]
                    com = row_2[3]
                    mon = 'Dolares'
                    regs = row_2[4]
                    agrupacion.append(ag)
                    prima.append(prn)
                    total.append(tot)
                    comision.append(com)
                    moneda.append(mon)
                    registrosA.append(regs)
            obj = {                
                'agrupacion'   : str(list(agrupacion)),
                'prima'   : str(list(prima)),
                'total'   : str(list(total)),
                'comision'   : str(list(comision)),
                'moneda'   : str(list(moneda)),
                'registrosA'   : str(list(registrosA)),
                'com_conciliada'   : str(list(com_conciliada)),
                'dolares'   : '$ {:,.2f}'.format(dolares),
                'pesos'   : '$ {:,.2f}'.format(p),
                'cpesos'   : '$ {:,.2f}'.format(c_pesos),
                'cdolares'   : '$ {:,.2f}'.format(c_dolares),
                'ccpesos'   : '$ {:,.2f}'.format(cc_pesos),
                'ccdolares'   : '$ {:,.2f}'.format(cc_dolares),
                'npesos'   : '$ {:,.2f}'.format(neta_pesos),
                'ndolares'   : '$ {:,.2f}'.format(neta_dolares),
                'grupo': int(group_by),
                'tipo_reporte': tipo_reporte,
                'email_org':info_org['email'],
                'phone_org':info_org['phone'],
                'webpage_org':info_org['webpage'],
                'address_org':info_org['address'],
                'urlname_org':info_org['name'],
            }   
            obj['columns'] = columns
            obj['imagen'] = archivo_imagen
            obj['registros'] = len(agrupacion) 
            obj['typeReport'] = 2
            r = requests.post(settings.SERVICEEXCEL_URL + 'get-polizaAgrupadoExcelReporte/', obj,
            # headers = {
            # 'Authorization':'Bearer %s'%request.user.user_info.fi_token_refresh,
            # # 'Content-Type':'application/json' 
            # }, 
            stream=True)

            return decide_how_send_report(es_asincrono, tipo_reporte, r, user)


            return response
        elif group_by == 4:
            prueba_n_pesos = pesos_poliza.values('natural__group__group_name').annotate(Count('natural__group')).annotate(Sum('p_neta')).annotate(Sum('p_total')).annotate(Sum('comision')).order_by('natural__group__group_name')
            prueba_n_dolares = dolares_poliza.values('natural__group__group_name').annotate(Count('natural__group')).annotate(Sum('p_neta')).annotate(Sum('p_total')).annotate(Sum('comision')).order_by('natural__group__group_name')
            prueba_j_pesos = pesos_poliza.values('juridical__group__group_name').annotate(Count('juridical__group')).annotate(Sum('p_neta')).annotate(Sum('p_total')).annotate(Sum('comision')).order_by('juridical__group__group_name')
            prueba_j_dolares = dolares_poliza.values('juridical__group__group_name').annotate(Count('juridical__group')).annotate(Sum('p_neta')).annotate(Sum('p_total')).annotate(Sum('comision')).order_by('juridical__group__group_name')
            prueba['naturals_pesos'] = prueba_n_pesos
            prueba['juridicals_pesos'] = prueba_j_pesos
            prueba['naturals_dolares'] = prueba_n_dolares
            prueba['juridicals_dolares'] = prueba_j_dolares
            rows_pesos = prueba['naturals_pesos'].values_list('natural__group__group_name','p_neta__sum', 'p_total__sum','comision__sum', 'natural__group__count')
            rows_dolares = prueba['naturals_dolares'].values_list('natural__group__group_name','p_neta__sum', 'p_total__sum','comision__sum', 'natural__group__count')
            rows_2_pesos = prueba['juridicals_pesos'].values_list('juridical__group__group_name','p_neta__sum', 'p_total__sum','comision__sum', 'juridical__group__count')
            rows_2_dolares = prueba['juridicals_dolares'].values_list('juridical__group__group_name','p_neta__sum', 'p_total__sum','comision__sum', 'juridical__group__count')
            tipo_reporte = tipo_reporte + " por Grupo"
            
            tipoContratante = []
            agrupacion = []
            prima = []
            total = []
            comision = []
            moneda = []
            registrosA = []
            com_conciliada = []
            dolares = 0
            p = 0
            c_pesos = 0
            cc_pesos = 0
            c_dolares = 0
            cc_dolares = 0
            neta_pesos = 0
            neta_dolares = 0
            for row in rows_pesos:
                if row[0] != None:                    
                    row_num += 1
                    c_pesos = c_pesos + row[3]
                    cc_pesos = cc_pesos 
                    neta_pesos = neta_pesos + row[1]
                    if row[2]:
                        p = p + row[2]
                    ag = ((((((row[0]).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'') 
                    tipoc = 'Naturales'   
                    prn = row[1]
                    tot = row[2]
                    com = row[3]
                    mon = 'Pesos'
                    regs = row[4]
                    agrupacion.append(ag)
                    tipoContratante.append(tipoc)
                    prima.append(prn)
                    total.append(tot)
                    comision.append(com)
                    moneda.append(mon)
                    registrosA.append(regs)
                    # com_conciliada.append(cc)
            # Pesos 2
            for row_1 in rows_2_pesos:
                pesos_total_f = 0
                if row_1[0] != None:
                    row_num += 1
                    c_pesos = c_pesos + row_1[3]
                    cc_pesos
                    neta_pesos = neta_pesos + row_1[1]
                    if row_1[2]:
                        p = p + row_1[2]                    
                    ag = ((((((row_1[0]).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')   
                    tipoc = 'Morales'
                    prn = row_1[1]
                    tot = row_1[2]
                    com = row_1[3]
                    mon = 'Pesos'
                    regs = row_1[4]
                    agrupacion.append(ag)
                    tipoContratante.append(tipoc)
                    prima.append(prn)
                    total.append(tot)
                    comision.append(com)
                    moneda.append(mon)
                    registrosA.append(regs)
                    # com_conciliada.append(cc)
            # dolares 1
            for row_2 in rows_dolares:
                if row_2[0] != None:
                    row_num += 1
                    c_dolares = c_dolares + row_2[3]
                    cc_dolares
                    neta_dolares = neta_dolares + row_2[1]
                    if row_2[2]:
                        dolares = dolares + row_2[2]
                    ag = ((((((row_2[0]).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')   
                    tipoc = 'Naturales'
                    prn = row_2[1]
                    tot = row_2[2]
                    com = row_2[3]
                    mon = 'Dolares'
                    regs = row_2[4]
                    agrupacion.append(ag)
                    tipoContratante.append(tipoc)
                    prima.append(prn)
                    total.append(tot)
                    comision.append(com)
                    moneda.append(mon)
                    registrosA.append(regs)
                    # com_conciliada.append(cc)
            # dolares 2               
            for row_2f in rows_2_dolares:
                if row_2f[0] != None:
                    row_num += 1
                    c_dolares = c_dolares + row_2f[3]
                    cc_dolares 
                    neta_dolares = neta_dolares + row_2f[1]
                    if row_2f[2]:
                        dolares = dolares + row_2f[2]
                    ag = ((((((row_2f[0]).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')   
                    tipoc = 'Morales'
                    prn = row_2f[1]
                    tot = row_2f[2]
                    com = row_2f[3]
                    mon = 'Dolares'
                    regs = row_2f[4]
                    # cc = row_2f[5]
                    agrupacion.append(ag)
                    tipoContratante.append(tipoc)
                    prima.append(prn)
                    total.append(tot)
                    comision.append(com)
                    moneda.append(mon)
                    registrosA.append(regs)
                    # com_conciliada.append(cc)
            obj = {                
                'agrupacion'   : str(list(agrupacion)),
                'tipoContratante': str(list(tipoContratante)),
                'prima'   : str(list(prima)),
                'total'   : str(list(total)),
                'comision'   : str(list(comision)),
                'moneda'   : str(list(moneda)),
                'registrosA'   : str(list(registrosA)),
                'com_conciliada'   : str(list(com_conciliada)),
                'dolares'   : '$ {:,.2f}'.format(dolares),
                'pesos'   : '$ {:,.2f}'.format(p),
                'cpesos'   : '$ {:,.2f}'.format(c_pesos),
                'cdolares'   : '$ {:,.2f}'.format(c_dolares),
                'ccpesos'   : '$ {:,.2f}'.format(cc_pesos),
                'ccdolares'   : '$ {:,.2f}'.format(cc_dolares),
                'npesos'   : '$ {:,.2f}'.format(neta_pesos),
                'ndolares'   : '$ {:,.2f}'.format(neta_dolares),
                'tipo_reporte': tipo_reporte,
                'grupo': int(group_by),
                'email_org':info_org['email'],
                'phone_org':info_org['phone'],
                'webpage_org':info_org['webpage'],
                'address_org':info_org['address'],
                'urlname_org':info_org['name'],
            }   
            obj['columns'] = columns
            obj['imagen'] = archivo_imagen
            obj['registros'] = len(agrupacion) 
            obj['typeReport'] = 2
            r = requests.post(settings.SERVICEEXCEL_URL + 'get-polizaAgrupadoExcelReporte/', obj,
            # headers = {
            # 'Authorization':'Bearer %s'%request.user.user_info.fi_token_refresh,
            # # 'Content-Type':'application/json' 
            # }, 
            stream=True)

            return decide_how_send_report(es_asincrono, tipo_reporte, r, user)

        elif group_by == 5:
            prueba['pesos'] = pesos_poliza.values('vendor__first_name', 'vendor__last_name').annotate(Count('vendor')).annotate(Sum('p_neta')).annotate(Sum('p_total')).annotate(Sum('comision')).order_by('vendor')
            prueba['dolares'] = dolares_poliza.values('vendor__first_name', 'vendor__last_name').annotate(Count('vendor')).annotate(Sum('p_neta')).annotate(Sum('p_total')).annotate(Sum('comision')).order_by('vendor')
            rows_pesos = prueba['pesos'].values_list('vendor__first_name','p_neta__sum', 'p_total__sum','comision__sum', 'vendor__count','vendor__last_name')
            rows_dolares = prueba['dolares'].values_list('vendor__first_name','p_neta__sum', 'p_total__sum','comision__sum', 'vendor__count','vendor__last_name')
            rows_2 =[]
            prefs_pesos = pesos_poliza.values('ref_policy__referenciador__first_name', 'ref_policy__referenciador__last_name').annotate(Count('ref_policy__referenciador'),Count('ref_policy__referenciador__id')).annotate(Sum('p_neta')).annotate(Sum('comision')).annotate(Sum('p_total')).order_by('ref_policy__referenciador__id')
            prefs_dols = dolares_poliza.values('ref_policy__referenciador__first_name', 'ref_policy__referenciador__last_name').annotate(Count('ref_policy__referenciador'),Count('ref_policy__referenciador__id')).annotate(Sum('p_neta')).annotate(Sum('comision')).annotate(Sum('p_total')).order_by('ref_policy__referenciador__id')
            rows_pesos_pr = prefs_pesos.values_list('ref_policy__referenciador__first_name','p_neta__sum', 'p_total__sum','comision__sum', 'ref_policy__referenciador__id__count','ref_policy__referenciador__last_name')
            rows_dolares_pr = prefs_dols.values_list('ref_policy__referenciador__first_name','p_neta__sum', 'p_total__sum','comision__sum', 'ref_policy__referenciador__id__count','ref_policy__referenciador__last_name')
            tipo_reporte = tipo_reporte + " por Referenciador" 
            # -------
            tipoContratante = []
            agrupacion = []
            prima = []
            total = []
            comision = []
            moneda = []
            registrosA = []
            com_conciliada = []
            dolares = 0
            p = 0
            c_pesos = 0
            cc_pesos = 0
            c_dolares = 0
            cc_dolares = 0
            neta_pesos = 0
            neta_dolares = 0
            for row in rows_pesos:
                if row[0] != None:                    # -------------------
                    row_num += 1
                    c_pesos = c_pesos + row[3]
                    cc_pesos = cc_pesos
                    neta_pesos = neta_pesos + row[1]
                    if row[2]:
                        p = p + row[2]
                    ag = row[0] + ' ' + str(row[5])
                    tipoc = 'Pólizas'   
                    prn = row[1]
                    tot = row[2]
                    com = row[3]
                    mon = 'Pesos'
                    regs = row[4]
                    agrupacion.append(ag)
                    tipoContratante.append(tipoc)
                    prima.append(prn)
                    total.append(tot)
                    comision.append(com)
                    moneda.append(mon)
                    registrosA.append(regs)
                    # com_conciliada.append(cc)
            for row_2 in rows_dolares:
                if row_2[0] != None:
                    row_num += 1
                    c_dolares = c_dolares + row_2[3]
                    cc_dolares
                    neta_dolares = neta_dolares + row_2[1]
                    if row_2[2]:
                        dolares = dolares + row_2[2]
                    ag = row_2[0] +' '+str(row_2[5])
                    tipoc = 'Pólizas'
                    prn = row_2[1]
                    tot = row_2[2]
                    com = row_2[3]
                    mon = 'Dólares'
                    regs = row_2[4]
                    agrupacion.append(ag)
                    tipoContratante.append(tipoc)
                    prima.append(prn)
                    total.append(tot)
                    comision.append(com)
                    moneda.append(mon)
                    registrosA.append(regs)
                    # com_conciliada.append(cc)
            # dolares 2 
            for row__2 in rows_pesos_pr:
                if row__2[0] != None:
                    row_num += 1
                    c_dolares = c_dolares + row__2[3]
                    cc_dolares
                    neta_dolares = neta_dolares + row__2[1]
                    if row__2[2]:
                        dolares = dolares + row__2[2]
                    ag = row__2[0] +' '+str(row__2[5])
                    tipoc = 'Pólizas'
                    prn = row__2[1]
                    tot = row__2[2]
                    com = row__2[3]
                    mon = 'Pesos'
                    regs = row__2[4]
                    agrupacion.append(ag)
                    tipoContratante.append(tipoc)
                    prima.append(prn)
                    total.append(tot)
                    comision.append(com)
                    moneda.append(mon)
                    registrosA.append(regs)
                    # com_conciliada.append(cc)
            for r1 in rows_dolares_pr:
                if r1[0] != None:
                    row_num += 1
                    c_dolares = c_dolares + r1[3]
                    cc_dolares
                    neta_dolares = neta_dolares + r1[1]
                    if r1[2]:
                        dolares = dolares + r1[2]
                    ag = r1[0] +' '+str(r1[5])
                    tipoc = 'Pólizas'
                    prn = r1[1]
                    tot = r1[2]
                    com = r1[3]
                    mon = 'Dólares'
                    regs = r1[4]
                    agrupacion.append(ag)
                    tipoContratante.append(tipoc)
                    prima.append(prn)
                    total.append(tot)
                    comision.append(com)
                    moneda.append(mon)
                    registrosA.append(regs)
                    # com_conciliada.append(cc) 
            obj = {                
                'agrupacion'   : str(list(agrupacion)),
                'tipoContratante': str(list(tipoContratante)),
                'prima'   : str(list(prima)),
                'total'   : str(list(total)),
                'comision'   : str(list(comision)),
                'moneda'   : str(list(moneda)),
                'registrosA'   : str(list(registrosA)),
                'com_conciliada'   : str(list(com_conciliada)),
                'dolares'   : '$ {:,.2f}'.format(dolares),
                'pesos'   : '$ {:,.2f}'.format(p),
                'cpesos'   : '$ {:,.2f}'.format(c_pesos),
                'cdolares'   : '$ {:,.2f}'.format(c_dolares),
                'ccpesos'   : '$ {:,.2f}'.format(cc_pesos),
                'ccdolares'   : '$ {:,.2f}'.format(cc_dolares),
                'npesos'   : '$ {:,.2f}'.format(neta_pesos),
                'ndolares'   : '$ {:,.2f}'.format(neta_dolares),
                'tipo_reporte': tipo_reporte,
                'grupo': int(group_by),
                'email_org':info_org['email'],
                'phone_org':info_org['phone'],
                'webpage_org':info_org['webpage'],
                'address_org':info_org['address'],
                'urlname_org':info_org['name'],
            }   
            obj['columns'] = columns
            obj['imagen'] = archivo_imagen
            obj['registros'] = len(agrupacion) 
            obj['typeReport'] = 2
            r = requests.post(settings.SERVICEEXCEL_URL + 'get-polizaAgrupadoExcelReporte/', obj,
            # headers = {
            # 'Authorization':'Bearer %s'%request.user.user_info.fi_token_refresh,
            # # 'Content-Type':'application/json' 
            # }, 
            stream=True)

            return decide_how_send_report(es_asincrono, tipo_reporte, r, user)

        elif group_by == 6:
            prueba['pesos'] = pesos_poliza.values('status').annotate(Count('status')).annotate(Sum('p_neta')).annotate(Sum('p_total')).annotate(Sum('comision')).order_by('status')
            prueba['dolares'] = dolares_poliza.values('status').annotate(Count('status')).annotate(Sum('p_neta')).annotate(Sum('p_total')).annotate(Sum('comision')).order_by('status')
            tipo_reporte = tipo_reporte + " por Estatus"
            rows_pesos = prueba['pesos'].values_list('status','p_neta__sum', 'p_total__sum','comision__sum', 'status__count')
            rows_dolares = prueba['dolares'].values_list('status','p_neta__sum', 'p_total__sum','comision__sum', 'status__count')
            rows_2 = []
       
            agrupacion = []
            prima = []
            total = []
            comision = []
            moneda = []
            registrosA = []
            com_conciliada = []
            dolares = 0
            p = 0
            c_pesos = 0
            cc_pesos = 0
            c_dolares = 0
            cc_dolares = 0
            neta_pesos = 0
            neta_dolares = 0
            for row in rows_pesos:
                if row[0] != None:
                    row_num += 1
                    c_pesos = c_pesos + row[3]
                    cc_pesos
                    neta_pesos = neta_pesos + row[1]
                    if row[2]:
                        p = p + row[2]
                    ag = checkStatusPolicy(row[0])
                    print(ag)
                    prn = row[1]
                    tot = row[2]
                    com = row[3]
                    mon = 'Pesos'
                    regs = row[4]
                    agrupacion.append(ag)
                    prima.append(prn)
                    total.append(tot)
                    comision.append(com)
                    moneda.append(mon)
                    registrosA.append(regs)
                    # com_conciliada.append(cc)
            # Pesos 2
            for row_2 in rows_dolares:
                if row_2[0] != None:
                    row_num += 1
                    c_dolares = c_dolares + row_2[3]
                    cc_dolares
                    neta_dolares = neta_dolares + row_2[1]
                    if row_2[2]:
                        dolares = dolares + row_2[2]
                    ag = checkStatusPolicy(row_2[0])
                    prn = row_2[1]
                    tot = row_2[2]
                    com = row_2[3]
                    mon = 'Dolares'
                    regs = row_2[4]
                    agrupacion.append(ag)
                    prima.append(prn)
                    total.append(tot)
                    comision.append(com)
                    moneda.append(mon)
                    registrosA.append(regs)
                    # com_conciliada.append(cc)
            # dolares 2               
            obj = {                
                'agrupacion'   : str(list(agrupacion)),
                'prima'   : str(list(prima)),
                'total'   : str(list(total)),
                'comision'   : str(list(comision)),
                'moneda'   : str(list(moneda)),
                'registrosA'   : str(list(registrosA)),
                'com_conciliada'   : str(list(com_conciliada)),
                'dolares'   : '$ {:,.2f}'.format(dolares),
                'pesos'   : '$ {:,.2f}'.format(p),
                'cpesos'   : '$ {:,.2f}'.format(c_pesos),
                'cdolares'   : '$ {:,.2f}'.format(c_dolares),
                'ccpesos'   : '$ {:,.2f}'.format(cc_pesos),
                'ccdolares'   : '$ {:,.2f}'.format(cc_dolares),
                'npesos'   : '$ {:,.2f}'.format(neta_pesos),
                'ndolares'   : '$ {:,.2f}'.format(neta_dolares),
                'grupo': int(group_by),
                'tipo_reporte': tipo_reporte,
                'email_org':info_org['email'],
                'phone_org':info_org['phone'],
                'webpage_org':info_org['webpage'],
                'address_org':info_org['address'],
                'urlname_org':info_org['name'],
            }   
            obj['columns'] = columns
            obj['imagen'] = archivo_imagen
            obj['registros'] = len(agrupacion) 
            obj['typeReport'] = 2
            r = requests.post(settings.SERVICEEXCEL_URL + 'get-polizaAgrupadoExcelReporte/', obj,
            # headers = {
            # 'Authorization':'Bearer %s'%request.user.user_info.fi_token_refresh,
            # # 'Content-Type':'application/json' 
            # }, 
            stream=True)
    
            return decide_how_send_report(es_asincrono, tipo_reporte, r, user)

        elif group_by == 7:
            tipo_reporte = tipo_reporte + " por Usuario"
            prueba['pesos'] = pesos_poliza.values('owner__first_name', 'owner__last_name').annotate(Count('owner')).annotate(Sum('p_neta')).annotate(Sum('p_total')).annotate(Sum('comision')).order_by('owner')
            prueba['dolares'] = dolares_poliza.values('owner__first_name', 'owner__last_name').annotate(Count('owner')).annotate(Sum('p_neta')).annotate(Sum('p_total')).annotate(Sum('comision')).order_by('owner')
            rows_pesos = prueba['pesos'].values_list('owner__first_name','p_neta__sum', 'p_total__sum','comision__sum', 'owner__count','owner__last_name')
            rows_dolares = prueba['dolares'].values_list('owner__first_name','p_neta__sum', 'p_total__sum','comision__sum', 'owner__count','owner__last_name')

            agrupacion = []
            prima = []
            total = []
            comision = []
            moneda = []
            registrosA = []
            com_conciliada = []
            dolares = 0
            p = 0
            c_pesos = 0
            cc_pesos = 0
            c_dolares = 0
            cc_dolares = 0
            neta_pesos = 0
            neta_dolares = 0
            for row in rows_pesos:
                if row[0] != None:
                    row_num += 1
                    c_pesos = c_pesos + row[3]
                    neta_pesos = neta_pesos + row[1]
                    if row[2]:
                        p = p + row[2]
                    ag = row[0]+ ' ' + str(row[5])  
                    prn = row[1]
                    tot = row[2]
                    com = row[3]
                    mon = 'Pesos'
                    regs = row[4]
                    agrupacion.append(ag)
                    prima.append(prn)
                    total.append(tot)
                    comision.append(com)
                    moneda.append(mon)
                    registrosA.append(regs)
                    # com_conciliada.append(cc)
            # dolares 1
            for row_2 in rows_dolares:
                if row_2[0] != None:
                    row_num += 1
                    c_dolares = c_dolares + row_2[3]
                    cc_dolares
                    neta_dolares = neta_dolares + row_2[1]
                    if row_2[2]:
                        dolares = dolares + row_2[2]
                    ag = row_2[0]+ ' ' + str(row_2[5]) 
                    prn = row_2[1]
                    tot = row_2[2]
                    com = row_2[3]
                    mon = 'Dolares'
                    regs = row_2[4]
                    agrupacion.append(ag)
                    prima.append(prn)
                    total.append(tot)
                    comision.append(com)
                    moneda.append(mon)
                    registrosA.append(regs)
                    # com_conciliada.append(cc)
            obj = {                
                'agrupacion'   : str(list(agrupacion)),
                'prima'   : str(list(prima)),
                'total'   : str(list(total)),
                'comision'   : str(list(comision)),
                'moneda'   : str(list(moneda)),
                'registrosA'   : str(list(registrosA)),
                'com_conciliada'   : str(list(com_conciliada)),
                'dolares'   : '$ {:,.2f}'.format(dolares),
                'pesos'   : '$ {:,.2f}'.format(p),
                'cpesos'   : '$ {:,.2f}'.format(c_pesos),
                'cdolares'   : '$ {:,.2f}'.format(c_dolares),
                'ccpesos'   : '$ {:,.2f}'.format(cc_pesos),
                'ccdolares'   : '$ {:,.2f}'.format(cc_dolares),
                'npesos'   : '$ {:,.2f}'.format(neta_pesos),
                'ndolares'   : '$ {:,.2f}'.format(neta_dolares),
                'grupo': int(group_by),
                'tipo_reporte': tipo_reporte,
                'email_org':info_org['email'],
                'phone_org':info_org['phone'],
                'webpage_org':info_org['webpage'],
                'address_org':info_org['address'],
                'urlname_org':info_org['name'],
            }   
            obj['columns'] = columns
            obj['imagen'] = archivo_imagen
            obj['registros'] = len(agrupacion) 
            obj['typeReport'] = 2
            r = requests.post(settings.SERVICEEXCEL_URL + 'get-polizaAgrupadoExcelReporte/', obj,
            # headers = {
            # 'Authorization':'Bearer %s'%request.user.user_info.fi_token_refresh,
            # # 'Content-Type':'application/json' 
            # }, 
            stream=True)

            return decide_how_send_report(es_asincrono, tipo_reporte, r, user)

        # ---------report service excel----
    # Excel no agrupado
    else:
        columns = ['Tipo', 'No.Póliza', 'Contratante','Grupo', 'Email', 'Teléfono', 'Proveedor', 'Subramo',
            'Forma de Pago', 'Estatus', 'Inicio Póliza', 'Fin Póliza', 'Moneda', 'Prima Neta', 
            'RPF', 'Derecho', 'IVA', 'Prima Total', 'Comisión', 'Clave', 'Sucursal', 'Referenciador', 'Creado por',
            'Fecha creación', 'Observaciones', 'Asegurado', 'Marca', 'Modelo', 'Versión', 'Año',
            'Placas', 'Motor', 'Adaptaciones', 'Conductor','Dirección','Código Postal','Identificador','Ejecutivo Cobranza',
            'Responsable','Ramo','Cancelacion','Renovable','OT/Póliza asociada','Origen Póliza','Paquete', 'Subgrupo','Subsubgrupo',
            'Agrupación','Subagrupación','Subsubagrupación','Clasificación','Línea Negocio']
            
        try:
                columns = data['cols1']
                if not 'Asegurado' in columns:
                        columns.insert(24,'Asegurado')
                if not 'Cancelacion' in columns:
                        columns.insert(43,'Motivo de Cancelación')
                if not 'Renovable' in columns:
                        columns.insert(44,'Renovable')
                if not 'OT/Póliza asociada' in columns:
                        columns.insert(45,'OT/Póliza asociada')
                if not 'Origen Póliza' in columns:
                        columns.insert(46,'Origen Póliza')
                if not 'Paquete' in columns:
                        columns.insert(47,'Paquete')
                fo = len(columns)
                if not 'SubGrupo' in columns:
                    columns.insert(int(fo),'Subgrupo')
                if not 'SubsubGrupo' in columns:
                    columns.insert(int(fo)+1,'Subsubgrupo')
                if not 'Agrupación' in columns:
                    columns.insert(int(fo)+2,'Agrupación')
                if not 'SubAgrupación' in columns:
                    columns.insert(int(fo)+3,'Subagrupación')
                if not 'SubsubAgrupación' in columns:
                    columns.insert(int(fo)+4,'Subsubagrupación')
                if not 'Clasificación' in columns:
                    columns.insert(int(fo)+5,'Clasificación')
                if not 'Línea de Negocio' in columns:
                    columns.insert(int(fo)+5,'Línea de Negocio')
        except:
            pass        
        # ******************************************
        # Empieza insertado de imagen
        if len(info_org['logo']) != 0:
          archivo_imagen = 'https://miurabox.s3.amazonaws.com/cas/' + info_org['logo']
        else:
          archivo_imagen = 'saam.jpg'
        # print(info_org)
            
        asegurado = []
        antiguedad = []
        referenciador = []
        creadopor = []
        dateRen = []
        otRen = []
        responsable = []
        contratante = []
        contratanteE = []
        contratanteP = []
        contratanteG = []
        collection = []
        clave = []
        cp = []
        paquete = []
        valueMA = []  
        valueModA = [] 
        valueVA = [] 
        valueYA = [] 
        valueLPA = [] 
        valueEA = [] 
        valueAdA = [] 
        valueDrA = []
        nueva = []
        grupo1 = []
        grupo2 = []
        nivelAg = []
        grupo3 = []
        grupo4 = []
        clasifica = []
        businessLine = []
        polizas = polizas.order_by('id')
        # polizas = polizas.order_by('natural__full_name','juridical__j_name')
        for r in polizas:   
            nuevaX = '' 
            if origen == 0:   
                if r in polizasNewAdd1:
                    nuevaX = 'Nueva'  
                if r in polizasNewAdd2:
                    nuevaX = 'Renovación'   
            if r.address:
               pc = r.address.postal_code
            else:
               pc = ''
            antig = get_antiguedad(r.start_of_validity)
            if r.owner:
                val_owner = r.owner.first_name + ' '+ str(r.owner.last_name)
            else:
                val_owner = ''
            if r.paquete:
                pac = r.paquete.package_name
            else:
                pac = ''
            if r.responsable:
                val_resp = r.responsable.first_name + ' '+ str(r.responsable.last_name)
            else:
                val_resp = ''
            if r.collection_executive:
                val_col = r.collection_executive.first_name + ' '+ str(r.collection_executive.last_name)
            else:
                val_col = ''
            if r.clave:
                try:
                    cve = r.clave.name + ' '+ str(r.clave.clave)
                    cve = ((((((cve).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                except Exception as ere:
                    cve = ''
            else:
                cve = ''
            if r.business_line:
                if int(r.business_line) ==1:
                    businessLine_ = 'Comercial'
                elif int(r.business_line) ==2:
                    businessLine_ = 'Personal'
                elif int(r.business_line) ==0:
                    businessLine_ = 'Otro'
                else:
                    businessLine_ = ''
            else:
                if int(r.business_line) ==0:
                    businessLine_ = 'Otro'
                else:
                    businessLine_ = ''
            if r.natural:
                contratan = ((((((r.natural.full_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                contratanE = r.natural.email
                contratanP = r.natural.phone_number
                # contratanG = r.natural.group.group_name
                if r.natural.classification:
                    clasifica_ = r.natural.classification.classification_name  
                else:
                    clasifica_='-----'
                contratanG = ((((((r.natural.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                try:
                    if r.natural.group.type_group == 1:
                        contratanG = ((((((r.natural.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo1_ = ''
                        grupo2_ = ''
                    elif r.natural.group.type_group == 2:
                        grupotype1 = Group.objects.get(pk = r.natural.group.parent.id)
                        contratanG = ((((((grupotype1.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo1_ = ((((((r.natural.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo2_ = ''
                    elif r.natural.group.type_group == 3:
                        grupotype1 = Group.objects.get(pk = r.natural.group.parent.parent.id)
                        contratanG = ((((((grupotype1.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        subgrupotype2 = Group.objects.get(pk = r.natural.group.parent.id)
                        grupo1_ = ((((((subgrupotype2.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo2_ = ((((((r.natural.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                except Exception as e:
                    contratanG = ''
                    grupo1_ = ''
                    grupo2_ = ''
                try:
                    if r.natural.grouping_level:
                        if r.natural.grouping_level.type_grouping == 1:
                            nivelAg_ = ((((((r.natural.grouping_level.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                            grupo3_ = ''
                            grupo4_ = ''
                        elif r.natural.grouping_level.type_grouping == 2:
                            grupotype1 = GroupingLevel.objects.get(pk = r.natural.grouping_level.parent.id)
                            nivelAg_ = ((((((grupotype1.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                            grupo3_ = ((((((r.natural.grouping_level.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                            grupo4_ = ''
                        elif r.natural.grouping_level.type_grouping == 3:
                            grupotype1 = GroupingLevel.objects.get(pk = r.natural.grouping_level.parent.parent.id)
                            nivelAg_ = ((((((grupotype1.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                            subgrupotype2 = GroupingLevel.objects.get(pk = r.natural.grouping_level.parent.id)
                            grupo3_ = ((((((subgrupotype2.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                            grupo4_ = ((((((r.natural.grouping_level.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    else:
                        nivelAg_ = ''
                        grupo3_ = ''
                        grupo4_ = ''  
                except Exception as e:
                    print('--dsdasd',e)
                    nivelAg_ = ''
                    grupo3_ = ''
                    grupo4_ = ''
            else:
                contratan = ((((((r.juridical.j_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                contratanE = r.juridical.email
                contratanP = r.juridical.phone_number
                # contratanG = r.juridical.group.group_name
                if r.juridical.classification:
                    clasifica_ = r.juridical.classification.classification_name  
                else:
                    clasifica_='-----'
                contratanG = ((((((r.juridical.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                try:
                    if r.juridical.group.type_group == 1:
                        contratanG = ((((((r.juridical.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo1_ = ''
                        grupo2_ = ''
                    elif r.juridical.group.type_group == 2:
                        grupotype1 = Group.objects.get(pk = r.juridical.group.parent.id)
                        contratanG = ((((((grupotype1.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo1_ = ((((((r.juridical.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo2_ = ''
                    elif r.juridical.group.type_group == 3:
                        grupotype1 = Group.objects.get(pk = r.juridical.group.parent.parent.id)
                        contratanG = ((((((grupotype1.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        subgrupotype2 = Group.objects.get(pk = r.juridical.group.parent.id)
                        grupo1_ = ((((((subgrupotype2.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo2_ = ((((((r.juridical.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                except Exception as e:
                    contratanG = ''
                    grupo1_ = ''
                    grupo2_ = ''
                try:
                    if r.juridical.grouping_level:
                        if r.juridical.grouping_level.type_grouping == 1:
                            nivelAg_ = ((((((r.juridical.grouping_level.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                            grupo3_ = ''
                            grupo4_ = ''
                        elif r.juridical.grouping_level.type_grouping == 2:
                            grupotype1 = GroupingLevel.objects.get(pk = r.juridical.grouping_level.parent.id)
                            nivelAg_ = ((((((grupotype1.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                            grupo3_ = ((((((r.juridical.grouping_level.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                            grupo4_ = ''
                        elif r.juridical.grouping_level.type_grouping == 3:
                            grupotype1 = GroupingLevel.objects.get(pk = r.juridical.grouping_level.parent.parent.id)
                            nivelAg_ = ((((((grupotype1.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                            subgrupotype2 = GroupingLevel.objects.get(pk = r.juridical.grouping_level.parent.id)
                            grupo3_ = ((((((subgrupotype2.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                            grupo4_ = ((((((r.juridical.grouping_level.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    else:
                        nivelAg_ = ''
                        grupo3_ = ''
                        grupo4_ = ''  
                except Exception as e:
                    print('--dsdasd',e)
                    nivelAg_ = ''
                    grupo3_ = ''
                    grupo4_ = ''
            try:
                old = OldPolicies.objects.filter(base_policy__id = r.id)
                try:
                  date_renovacion = old[0].created_at.strftime("%d/%m/%Y")
                except Exception as rr:
                    date_renovacion = 'Por renovar'
                try:
                    ot_renovacion = old[0].new_policy.poliza_number if old[0].new_policy.poliza_number else old[0].new_policy.internal_number
                except Exception as rr:
                    ot_renovacion = 'Por renovar'
            except Exception as dwe:
                date_renovacion = 'Por renovar'
                ot_renovacion = 'Por renovar'
            try:
                refs_policy = ReferenciadoresInvolved.objects.filter(policy = r.id,is_changed=False).values_list('referenciador__first_name','referenciador__last_name')
                if len(refs_policy):
                    v = []
                    try:
                        for it in refs_policy:
                            v.append(it[0] +' '+ str(it[1])+str('; '))
                        referenc = v
                    except Exception as y:
                        referenc = []
                        print('-f----cobexc--',y)
                else:
                    referenc = []
            except Exception as e:
                print('::::::::',e)
                referenc = []
            if r.ramo.ramo_code == 1:
                form = Life.objects.filter(policy = r.id)
                if form:
                    try:
                        if form[0].personal:
                            value = form[0].personal.full_name
                        else:
                            value = ''
                    except Exception as e:
                        print('--------e',e)
                        value = ''
                    valueM = '' 
                    valueMod = '' 
                    valueV = '' 
                    valueY = ''
                    valueLP = '' 
                    valueE = '' 
                    valueAd = '' 
                    valueDr = ''
                else:
                    value = ''
                    valueM = '' 
                    valueMod = '' 
                    valueV = '' 
                    valueY = ''
                    valueLP = '' 
                    valueE = '' 
                    valueAd = '' 
                    valueDr = ''
            elif r.ramo.ramo_code == 2:
                form = AccidentsDiseases.objects.filter(policy = r.id)
                if form:
                    value = form[0].personal.full_name
                    valueM = '' 
                    valueMod = '' 
                    valueV = '' 
                    valueY = ''
                    valueLP = '' 
                    valueE = '' 
                    valueAd = '' 
                    valueDr = ''
                else:
                    value = ''
                    valueM = '' 
                    valueMod = '' 
                    valueV = '' 
                    valueY = ''
                    valueLP = '' 
                    valueE = '' 
                    valueAd = '' 
                    valueDr = ''
            elif r.subramo.subramo_code == 9:
                form = AutomobilesDamages.objects.filter(policy = r.id)
                if form:
                    try:
                        form[0].model = ((((((form[0].model).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        form[0].version = ((((((form[0].version).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    except Exception as ers:
                        form[0].model = form[0].model
                        form[0].version = form[0].version
                    value = str(form[0].brand) + '-' + str(form[0].model )+ '-' + str(form[0].version)+ '-' + str(form[0].serial)
                    valueM = form[0].brand 
                    valueMod = form[0].model 
                    valueV = form[0].version 
                    valueY = form[0].year 
                    valueLP = form[0].license_plates 
                    valueE = form[0].engine 
                    valueAd = form[0].adjustment 
                    valueDr = form[0].driver 
                else:
                    value = ''
                    valueM = '' 
                    valueMod = '' 
                    valueV = '' 
                    valueY = ''
                    valueLP = '' 
                    valueE = '' 
                    valueAd = '' 
                    valueDr = ''
            elif r.ramo.ramo_code == 3 and not r.subramo.subramo_code == 9:
                form = Damages.objects.filter(policy = r.id)
                if form:
                    value = form[0].insured_item
                    valueM = '' 
                    valueMod = '' 
                    valueV = '' 
                    valueY = ''
                    valueLP = '' 
                    valueE = '' 
                    valueAd = '' 
                    valueDr = ''
                else:
                    value = ''
                    valueM = '' 
                    valueMod = '' 
                    valueV = '' 
                    valueY = ''
                    valueLP = '' 
                    valueE = '' 
                    valueAd = '' 
                    valueDr = ''
            if value:
                value = ((((((value).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'') 
            else:
                value = value
            
            if origen == 0:
                nueva.append(nuevaX)
            elif origen == 0:
                nueva.append('')
            elif origen == 1:
                nueva.append('Nueva')
            elif origen == 2:
                nueva.append('Renovación')
            else:
                nueva.append('')
            asegurado.append(value)
            dateRen.append(date_renovacion)
            otRen.append(ot_renovacion)
            referenciador.append(referenc)
            antiguedad.append(antig)
            creadopor.append(val_owner)
            contratante.append(contratan)
            contratanteE.append(contratanE)
            contratanteP.append(contratanP)
            contratanteG.append(contratanG)
            responsable.append(val_resp)
            collection.append(val_col)
            paquete.append(pac)
            clave.append(cve)
            cp.append(pc)
            valueMA.append(valueM) 
            valueModA.append(valueMod)  
            valueVA.append(valueV) 
            valueYA.append(valueY) 
            valueLPA.append(valueLP)  
            valueEA.append(valueE)  
            valueAdA.append(valueAd)  
            valueDrA.append(valueDr) 
            # ---
            grupo1.append(grupo1_)
            grupo2.append(grupo2_)
            nivelAg.append(nivelAg_)
            grupo3.append(grupo3_)
            grupo4.append(grupo4_)
            clasifica.append(clasifica_)
            businessLine.append(businessLine_)
            # ----
        obj = {
            'reasoncacel'   : str(list(polizas.values_list('reason_cancel', flat = True))),
            'renewable'   : str(list(polizas.values_list('is_renewable', flat = True))),
            'document_type'   : str(list(polizas.values_list('document_type', flat = True))),
            'poliza_number'   : str(list(polizas.values_list('poliza_number', flat = True))),
            'contratante'   : str(list(contratante)),
            'contratanteE'   : str(list(contratanteE)),
            'contratanteP'   : str(list(contratanteP)),
            'contratanteG'   : str(list(contratanteG)),
            'aseguradora__compania'   : str(list(polizas.values_list('aseguradora__alias', flat = True))),
            'subramo__subramo_name'   : str(list(polizas.values_list('subramo__subramo_name', flat = True))),
            'ramo'   : str(list(polizas.values_list('ramo__ramo_name', flat = True))),
            'forma_de_pago'   : str(list(polizas.values_list('forma_de_pago', flat = True))),
            'status'   : str(list(polizas.values_list('status', flat = True))),
            'start_of_validity'   : str(list([date.strftime("%d/%m/%Y") if date else '' for date in polizas.values_list('start_of_validity', flat = True) ])) ,
            'end_of_validity'   : str(list([date.strftime("%d/%m/%Y") if date else '' for date in polizas.values_list('end_of_validity', flat = True) ])) ,
            'f_currency'   : str(list(polizas.values_list('f_currency', flat = True))),
            'p_neta'   : str(list(polizas.values_list('p_neta', flat = True))),
            'rpf'   : str(list(polizas.values_list('rpf', flat = True))),
            'derecho'   : str(list(polizas.values_list('derecho', flat = True))),
            'iva'   : str(list(polizas.values_list('iva', flat = True))),
            'p_total'   : str(list(polizas.values_list('p_total', flat = True))),
            'comision'   : str(list(polizas.values_list('comision', flat = True))),
            '_id'   : str(list(polizas.values_list('id', flat = True))),
            'observations'   : str(list([(((((((x).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')) if x else '' for x in polizas.values_list('observations', flat = True)])),
            'created_at'   : str(list([date.strftime("%d/%m/%Y") if date else '' for date in polizas.values_list('created_at', flat = True) ]))  ,
            'clave'   : str(list(clave)),
            'sucursal__sucursal_name'   : str(list(polizas.values_list('sucursal__sucursal_name', flat = True))),
            'renewed_status': str(list(polizas.values_list('renewed_status', flat = True))),
            'paquete': str(list(paquete)),
            'identifier'   : str(list([(((((((x).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')) if x else '' for x in polizas.values_list('identifier', flat = True)])),
            'address': str(list(polizas.values_list('address__administrative_area_level_1', flat = True))),
            'cp': str(list(cp)),
            'dateRen'   : str(list(dateRen)),
            'collection'   : str(list(collection)),
            'otRen'   : str(list(otRen)),
            'asegurado'   : str(list(asegurado)),
            'marca'   : str(list(valueMA)),
            'modelo'   : str(list(valueModA)),
            'version'   : str(list(valueVA)),
            'anio'   : str(list(valueYA)),
            'placa'   : str(list(valueLPA)),
            'motor'   : str(list(valueEA)),
            'adaptaciones'   : str(list(valueAdA)),
            'conductor'   : str(list(valueDrA)),
            'referenciador'   : str(list(referenciador)),
            'antiguedad'   : str(list(antiguedad)),
            'creadopor'   : str(list(creadopor)),
            'responsable'   : str(list(responsable)),
            'origen'   : str(list(nueva)),
            'grupo1'   : str(list(grupo1)) ,
            'grupo2'   : str(list(grupo2)) ,
            'nivelAg'   : str(list(nivelAg)) ,
            'grupo3'   : str(list(grupo3)) ,
            'grupo4'   : str(list(grupo4)) ,
            'clasifica'   : str(list(clasifica)) ,
            'businessLine'   : str(list(businessLine)) ,
            'tipo_reporte': tipo_reporte,
            'email_org':info_org['email'],
            'phone_org':info_org['phone'],
            'webpage_org':info_org['webpage'],
            'address_org':info_org['address'],
            'urlname_org':info_org['name'],
            }  
        obj['columns'] = str(list(columns))
        obj['imagen'] = archivo_imagen
        obj['registros'] = len(polizas)
        r = requests.post(settings.SERVICEEXCEL_URL + 'get-polizasExcelReporte/', obj,
        # headers = {
        # 'Authorization':'Bearer %s'%request.user.user_info.fi_token_refresh,
        # # 'Content-Type':'application/json' 
        # }, 
        stream=True)

        return decide_how_send_report(es_asincrono, tipo_reporte, r, user)



#@shared_task(bind=True)
@app.task(bind=True, queue=settings.QUEUE)
def certificate_report_task(self,post,k):
    num_certificado = post.get('num_certificado')
    num_poliza = post.get('num_poliza')
    select_provider = k.get('select_provider')
    select_ramo = k.get('select_ramo')
    select_sramo = k.get('select_sramo')
    select_user = k.get('select_user')
    celula = post.get('celula')
    status_cert = post.get('status_cert')
    observaciones = post.get('observaciones')
    vendedor = post.get('vendedor')
    status_ren_cert = post.get('status_ren_cert')
    since = post.get('since')
    until = post.get('until')
    date_filter_by = int(post.get('date_filter_by'))
    org = post.get('org')
    email = post.get('email')
    contratante = int(post.get('contratante',0))
    contributory = int(post.get('contributory',0))

    permiso_correos = post.get('permiso_correos')
    # *******
    filters = []
    if since and date_filter_by != 2: #date_filter_by = 2 quita el filtro de fecha
        try:
            start_since = datetime.strptime(since, '%d/%m/%Y')
        except Exception as es:
            print('es',es)
            start_since = since
        if date_filter_by == 0 :
            filters.append(Q(parent__parent__parent__start_of_validity__gte = start_since))
        else:
            filters.append(Q(start_of_validity__gte = start_since))


    if until and date_filter_by != 2: #date_filter_by = 2 quita el filtro de fecha
        try:
            end_until = datetime.strptime(until, '%d/%m/%Y')
        except Exception as cv:
            end_until = until
        if date_filter_by == 0 :
            filters.append(Q(parent__parent__parent__start_of_validity__lt = end_until))
        else:
            filters.append(Q(start_of_validity__lt = end_until))


    if contratante:
        filters.append(Q(parent__parent__parent__contractor__id = contratante))

    if num_poliza:
        # filters.append(Q(poliza_number__icontains = num_poliza))
        filters.append((Q(poliza_number__icontains = num_poliza) | Q(parent__parent__parent__poliza_number__icontains = num_poliza)))

    if num_certificado:
        filters.append(Q(certificate_number__icontains = num_certificado))

    if select_provider:
        filters.append((Q(aseguradora__in = list(select_provider)) | Q(parent__parent__parent__aseguradora__in = list(select_provider))))            

    if select_ramo:
        filters.append((Q(ramo__ramo_code__in = list(select_ramo)) | Q(parent__parent__parent__ramo__ramo_code__in = list(select_ramo))))            

    if select_sramo:
        filters.append((Q(subramo__subramo_code__in = list(select_sramo)) | Q(parent__parent__parent__subramo__subramo_code__in = list(select_sramo))))

    if select_user:
        filters.append(Q(owner__id__in = list(select_user)))

    if celula and celula !=0 and celula != '0':
        filters.append(Q(parent__parent__parent__celula = celula))


    if status_cert:
        if status_cert == 1 or status_cert == '1':
            filters.append(Q(certificado_inciso_activo = True))
        elif status_cert == 2 or status_cert == '2':
            filters.append(Q(certificado_inciso_activo = False))


    if status_ren_cert:
        if status_ren_cert == 1 or status_ren_cert == '1':
            filters.append(Q(parent__parent__parent__renewed_status__in = [1,2]))
        elif status_cert == 2 or status_cert == '2':
            filters.append(Q(parent__parent__parent__renewed_status = 0))   

    if vendedor and int(vendedor) != 0:
        polizas_vendedor = ReferenciadoresInvolved.objects.filter(referenciador__id = vendedor, org_name = org,is_changed=False).values_list('policy', flat = True)
        filters.append(Q(parent__parent__parent__id__in = list(polizas_vendedor))) 
    if len(filters) > 0:
        certificados = Polizas.objects.filter(org_name = org, document_type = 6).exclude(parent__parent__parent__status = 0).exclude(status = 0).filter(reduce(and_, filters)).order_by('parent__parent__parent__poliza_number')
    else:
        certificados = Polizas.objects.filter(org_name = org, document_type = 6).exclude(parent__parent__parent__status = 0).exclude(status = 0).order_by('parent__parent__parent__poliza_number')

    try:
        dataToFilter = getDataForPerfilRestricted(post.get('user'), org)
    except Exception as er:
        dataToFilter = {}
        print('--error datat Restricted--',er)
    if dataToFilter:
        # Contratantes***
        polizasCl = Polizas.objects.filter(document_type__in = [3,6], org_name = org)
        polizasToF = Polizas.objects.filter(document_type__in = [3], org_name = org)
        if dataToFilter['ccpr']:
            polizasToF = polizasToF.filter(contractor__in = list(dataToFilter['ccpr']))
        if dataToFilter['cgpr']:
            polizasToF = polizasToF.filter(contractor__group__in = list(dataToFilter['cgpr']))
        if dataToFilter['ccepr']:
            polizasToF = polizasToF.filter(contractor__cellule__in = list(dataToFilter['ccepr']))
        if dataToFilter['crpr']:
            polizasToF = polizasToF.filter(contractor__vendor__in = list(dataToFilter['crpr']))
        if dataToFilter['cspr']:
            polizasToF = polizasToF.filter(contractor__sucursal__in = list(dataToFilter['cspr']))
        # Pólizas ****
        if dataToFilter['pppr']:
            polizasToF = polizasToF.filter(pk__in = list(dataToFilter['pppr']))
        if dataToFilter['pgpr']:
            polizasToF = polizasToF.filter(contractor__group__in = list(dataToFilter['pgpr']))
        if dataToFilter['pcepr']:
            polizasToF = polizasToF.filter(celula__in = list(dataToFilter['pcepr']))
        if dataToFilter['prpr']:
            polizasToF = polizasToF.filter(ref_policy__referenciador__in = list(dataToFilter['prpr']))
        if dataToFilter['pspr']:
            polizasToF = polizasToF.filter(sucursal__in = list(dataToFilter['pspr']))
        if dataToFilter['papr']:
            polizasToF = polizasToF.filter(groupinglevel__in = list(dataToFilter['papr']))
        if dataToFilter['pcapr']:
            polizasToF = polizasToF.filter(clave__in = list(dataToFilter['pcapr']))
        if dataToFilter['psrpr']:
            polizasToF = polizasToF.filter(subramo__subramo_code__in = list(dataToFilter['psrpr']))
        if dataToFilter['paspr']:
            polizasToF = polizasToF.filter(aseguradora__in = list(dataToFilter['paspr']))
        if dataToFilter['pstpr']:
            polizasToF = polizasToF.filter(status__in = list(dataToFilter['pstpr']))
        
        polizasGT = polizasCl.filter(parent__parent__parent__in = list(polizasToF))
        polizasFin = list(polizasGT.values_list('pk', flat = True))
        certificados = certificados.filter(pk__in = list(polizasFin))

    if observaciones and len(observaciones) > 0:
        certificados = certificados.filter(observations__icontains = observaciones)
    if contributory == 1 or contributory == '1':
        pgrupo = certificados.filter(contributory = True).values_list('parent__parent__parent__id',flat=True)       
        certificados = certificados.filter(parent__parent__parent__id__in = pgrupo)
    else:
        certificados = certificados
    filename = 'reporte_certificados_%s_%s_%s.xlsx'%(org, str(since).replace('/',''), str(until).replace('/',''))
    tipos = []
    marcas = []
    modelos = []
    anios = []
    versiones = []
    series = []
    motores = []
    drivers = []

    nombres = []
    sexos = []
    correos = []
    fechas_de_antiguedad = []


    poliza_number = []
    cerficate_number = []
    status = []
    ramo_name = []
    subramo_name = []
    aseguradora_alias = []
    package_name = []
    vigencia_inicial = []
    vigencia_final = []

    ivas = []
    recargos = []
    derechos = []
    primas_neta = []
    primas_total = []
    contratantes = []
    ids = []
    ivasNDev = []
    recargosNDev = []
    derechosNDev = []
    primas_netaNDev = []
    primas_totalNDev = []
    
    fechaAlta = []
    fechas_nacimiento = []
    fechas_baja = []
    endosos_baja = []
    endosos_alta = []
    adjuntos = []
    celula_poliza = []
    estatus_poliza = []
    observaciones = []

    contributoria = []
    rfccve = []
    rfchomocve = []
    dom_cp = []
    dom_poblacion = []
    dom_estado = []
    dom_callenum = []
    dom_colonia = []

    vigipoliza = []
    vigfpoliza = []
    statusCertsss = []
    subgrupoparent = []
    categoriaparent = []
    foliint = []
    fechacre = []
    for c in certificados:
        ids.append(c.id)
        adj = 'No'
        if PolizasFile.objects.filter(owner__id = c.id, org_name= c.org_name).exists():
            adj = 'Si'
        else:
            adj  ='No'
        caratula = Polizas.objects.get(id = c.caratula)

        if c.observations:
            observaciones.append(c.observations)
        else:
            observaciones.append('')

        try:
            subgrupoparent.append(c.parent.parent.name)
            categoriaparent.append(c.parent.name)
        except:
            subgrupoparent.append('')
            categoriaparent.append('')

        poliza_number.append(caratula.poliza_number)
        if caratula.contractor:
            contratantes.append(caratula.contractor.full_name)
        else:
            contratantes.append('Sin Contratante')
        cerficate_number.append(c.certificate_number)

        if c.certificado_inciso_activo:
            status.append('Activo')
        else:
            status.append('Inactivo')


        if c.parent and c.parent.parent and c.parent.parent.parent and c.parent.parent.parent.celula:
            celula_poliza.append(c.parent.parent.parent.celula.celula_name)
        else:
            celula_poliza.append("Sin célula")
        

        try:
            estatus_poliza.append( c.parent.parent.parent.get_status_display() )
        except:
            estatus_poliza.append( "" )
        try:
            statusCertsss.append( c.get_status_display() )
        except:
            statusCertsss.append( "" )          

        ramo_name.append(caratula.ramo.ramo_name)
        subramo_name.append(caratula.subramo.subramo_name)
        aseguradora_alias.append(caratula.aseguradora.alias)
          
        if c.paquete:
            package_name.append(c.paquete.package_name)
        else:
            package_name.append('No especificado')
        
        if caratula.start_of_validity:
            vigipoliza.append(caratula.start_of_validity.strftime("%d/%m/%Y"))
        else:
            vigipoliza.append(caratula.start_of_validity)
        if caratula.end_of_validity:
            vigfpoliza.append(caratula.end_of_validity.strftime("%d/%m/%Y"))
        else:
            vigfpoliza.append(caratula.end_of_validity) 

        if c.start_of_validity:
            vigencia_inicial.append(c.start_of_validity.strftime("%d/%m/%Y"))
        else:
            vigencia_inicial.append(c.start_of_validity)
        if c.end_of_validity:
            vigencia_final.append(c.end_of_validity.strftime("%d/%m/%Y"))
        else:
            vigencia_final.append(c.end_of_validity)      

        life = Life.objects.filter(policy = c)
        acc = AccidentsDiseases.objects.filter(policy = c)
        autos = AutomobilesDamages.objects.filter(policy = c)
        danioss = Damages.objects.filter(policy = c)


        alta_endoso = EndorsementCert.objects.filter(certificate = c, org_name = org, endorsement__endorsement_type = 'A', endorsement__status =2).order_by('-endorsement_id')
        if alta_endoso.exists():
            endosos_alta.append(alta_endoso.first().endorsement.number_endorsement)
            if c.charge_date:
                fechaAlta.append(c.charge_date.strftime("%d/%m/%Y"))
            else:
                fechaAlta.append('') 
        else:
            fechaAlta.append('') 
            endosos_alta.append('')

        baja_endoso = EndorsementCert.objects.filter(certificate = c, org_name = org, endorsement__endorsement_type = 'D', endorsement__status =2).order_by('-endorsement_id')
        if baja_endoso.exists():
            endosos_baja.append(baja_endoso.first().endorsement.number_endorsement)
        else:
            endosos_baja.append('') 

        if life.exists():
            tipos.append('Titular')
            nombres.append(life.first().personal.full_name)
            foliint.append(life.first().personal.id if life.first() and life.first().personal else '')
            fechacre.append(life.first().personal.created_at.strftime("%d/%m/%Y") if life.first() and life.first().personal and life.first().personal.created_at else '')
            if life.first().personal.birthdate:
                fechas_nacimiento.append(life.first().personal.birthdate.strftime("%d/%m/%Y"))
            else:
                fechas_nacimiento.append('')

            if permiso_correos == True or permiso_correos == 'True':
                correos.append(life.first().personal.email)
            else:
                correos.append('')

            if life.first().personal.antiguedad:
                fechas_de_antiguedad.append(life.first().personal.antiguedad.strftime("%d/%m/%Y"))
            else:
                fechas_de_antiguedad.append(life.first().personal.antiguedad)


            if life.first().personal.sex == 'M':
                sexos.append('Masculino')
            elif life.first().personal.sex == 'F':
                sexos.append('Femenino')
            else:
                sexos.append('No definido')

            if c.certificado_inciso_activo ==False:
                ivasNDev.append(life.first().personal.iva_earned)
                recargosNDev.append(life.first().personal.rpf_earned)
                derechosNDev.append(life.first().personal.derecho_earned)
                primas_netaNDev.append(life.first().personal.p_neta_earned)
                primas_totalNDev.append(life.first().personal.p_total_earned)
            else:
                ivasNDev.append(0)
                recargosNDev.append(0)
                derechosNDev.append(0)
                primas_netaNDev.append(0)
                primas_totalNDev.append(0)

            if life.first().personal.discharge_date:
                fechas_baja.append(life.first().personal.discharge_date.strftime("%d/%m/%Y"))
            else:
                if c.fecha_baja_inciso:
                    fechas_baja.append(c.fecha_baja_inciso.strftime("%d/%m/%Y"))
                else:
                    fechas_baja.append('')

            #ivas.append(life.first().personal.iva)
            #recargos.append(life.first().personal.rpf)
            #derechos.append(life.first().personal.derecho)
            #primas_neta.append(life.first().personal.p_neta)
            #primas_total.append(life.first().personal.p_total)
            ivas.append(c.iva)
            recargos.append(c.rpf)
            derechos.append(c.derecho)
            primas_neta.append(c.p_neta)
            primas_total.append(c.p_total)

            marcas.append('')
            modelos.append('')
            anios.append('')
            versiones.append('')
            series.append('')
            motores.append('')
            drivers.append('')
            adjuntos.append(adj)
            contributoria.append('No Aplica')  
            rfccve.append('')
            rfchomocve.append('')
            dom_cp.append('')
            dom_poblacion.append('')
            dom_estado.append('')
            dom_callenum.append('')
            dom_colonia.append('') 
            for rel in Beneficiaries.objects.filter(life = life.first()): 
                foliint.append(rel.id if rel else '')
                fechacre.append(rel.created_at.strftime("%d/%m/%Y") if rel and rel.created_at else '')
                contributoria.append('No Aplica')  
                rfccve.append('')
                rfchomocve.append('')
                dom_cp.append('')
                dom_poblacion.append('')
                dom_estado.append('')
                dom_callenum.append('')
                dom_colonia.append('')               
                poliza_number.append(caratula.poliza_number)
                if caratula.contractor:
                    contratantes.append(caratula.contractor.full_name)
                else:
                    contratantes.append('Sin Contratante')
                cerficate_number.append(c.certificate_number)
                if rel.is_active:
                    status.append('Activo')
                else:
                    status.append('Inactivo')
                observaciones.append('')
                ids.append('')
                try:
                    subgrupoparent.append(c.parent.parent.name)
                    categoriaparent.append(c.parent.name)
                except:
                    subgrupoparent.append('')
                    categoriaparent.append('')

                if c.parent and c.parent.parent and c.parent.parent.parent.celula:
                    celula_poliza.append(c.parent.parent.parent.celula.celula_name)
                else:
                    celula_poliza.append("Sin célula")               

                try:
                    estatus_poliza.append( c.parent.parent.parent.get_status_display() )
                except:
                    estatus_poliza.append( "" )
                try:
                    statusCertsss.append( c.get_status_display() )
                except:
                    statusCertsss.append( "" )

                ivasNDev.append(0)
                recargosNDev.append(0)
                derechosNDev.append(0)
                primas_netaNDev.append(0)
                primas_totalNDev.append(0)

                if rel.discharge_date:
                    fechas_baja.append(rel.discharge_date.strftime("%d/%m/%Y"))
                else:
                    if c.fecha_baja_inciso:
                        fechas_baja.append(c.fecha_baja_inciso.strftime("%d/%m/%Y"))
                    else:
                        fechas_baja.append('')

                if rel.birthdate:
                    fechas_nacimiento.append(rel.birthdate.strftime("%d/%m/%Y"))
                else:
                    fechas_nacimiento.append('')
                if rel.charge_date:
                    fechaAlta.append(rel.charge_date.strftime("%d/%m/%Y"))
                else:
                    if c.charge_date:
                        fechaAlta.append(c.charge_date.strftime("%d/%m/%Y"))    
                    else:
                        fechaAlta.append('')

                alta_endoso = EndorsementCert.objects.filter(beneficiarie = rel, org_name = org, endorsement__endorsement_type = 'A', endorsement__status =2).order_by('-endorsement_id')
                if alta_endoso.exists():
                    endosos_alta.append(alta_endoso.first().endorsement.number_endorsement)                  
                else:
                    endosos_alta.append('')

                baja_endoso = EndorsementCert.objects.filter(beneficiarie = rel, org_name = org, endorsement__endorsement_type = 'D', endorsement__status =2).order_by('-endorsement_id')
                if baja_endoso.exists():
                    endosos_baja.append(baja_endoso.first().endorsement.number_endorsement)
                else:
                    endosos_baja.append('')


                ivas.append(c.iva)
                recargos.append(c.rpf)
                derechos.append(c.derecho)
                primas_neta.append(c.p_neta)
                primas_total.append(c.p_total)


                if c.start_of_validity:
                    vigencia_inicial.append(c.start_of_validity.strftime("%d/%m/%Y"))
                else:
                    vigencia_inicial.append(c.start_of_validity)
                if c.end_of_validity:
                    vigencia_final.append(c.end_of_validity.strftime("%d/%m/%Y"))
                else:
                    vigencia_final.append(c.end_of_validity)
                if caratula.start_of_validity:
                    vigipoliza.append(caratula.start_of_validity.strftime("%d/%m/%Y"))
                else:
                    vigipoliza.append(caratula.start_of_validity)
                if caratula.end_of_validity:
                    vigfpoliza.append(caratula.end_of_validity.strftime("%d/%m/%Y"))
                else:
                    vigfpoliza.append(caratula.end_of_validity) 

                ramo_name.append(caratula.ramo.ramo_name)
                subramo_name.append(caratula.subramo.subramo_name)
                aseguradora_alias.append(caratula.aseguradora.alias)
                if caratula.paquete:
                    package_name.append(caratula.paquete.package_name)
                else:
                    package_name.append('No especificado')           

                tipos.append('Beneficiario')
                
                nombres.append(rel.full_name)
                correos.append('')
                if rel.antiguedad:
                    fechas_de_antiguedad.append(rel.antiguedad.strftime("%d/%m/%Y"))
                else:
                    fechas_de_antiguedad.append(rel.antiguedad)

                if rel.sex == 'M':
                    sexos.append('Masculino')
                elif rel.sex == 'F':
                    sexos.append('Femenino')
                else:
                    sexos.append('No definido')

                marcas.append('')
                modelos.append('')
                anios.append('')
                versiones.append('')
                series.append('')
                motores.append('')
                drivers.append('')
                adjuntos.append(adj)
        elif acc.exists():
            tipos.append('Titular')
            nombres.append(acc.first().personal.full_name)
            foliint.append(acc.first().personal.id if acc.first() and acc.first().personal else '')
            fechacre.append(acc.first().personal.created_at.strftime("%d/%m/%Y") if acc.first() and acc.first().personal and acc.first().personal.created_at else '')

            if acc.first().personal.birthdate:
                fechas_nacimiento.append(acc.first().personal.birthdate.strftime("%d/%m/%Y"))
            else:
                fechas_nacimiento.append('')

            
            if permiso_correos == True or permiso_correos == 'True':
                correos.append(acc.first().personal.email)
            else:
                correos.append('')


            if acc.first().personal.antiguedad:
                fechas_de_antiguedad.append(acc.first().personal.antiguedad.strftime("%d/%m/%Y"))
            else:
                fechas_de_antiguedad.append(acc.first().personal.antiguedad)

            if acc.first().personal.sex == 'M':
                sexos.append('Masculino')
            elif acc.first().personal.sex == 'F':
                sexos.append('Femenino')
            else:
                sexos.append('No definido')

            if c.certificado_inciso_activo ==False:
                ivasNDev.append(acc.first().personal.iva_earned)
                recargosNDev.append(acc.first().personal.rpf_earned)
                derechosNDev.append(acc.first().personal.derecho_earned)
                primas_netaNDev.append(acc.first().personal.p_neta_earned)
                primas_totalNDev.append(acc.first().personal.p_total_earned)
            else:
                ivasNDev.append(0)
                recargosNDev.append(0)
                derechosNDev.append(0)
                primas_netaNDev.append(0)
                primas_totalNDev.append(0)
            
            if acc.first().personal.discharge_date:
                fechas_baja.append(acc.first().personal.discharge_date.strftime("%d/%m/%Y"))
            else:
                if c.fecha_baja_inciso:
                    fechas_baja.append(c.fecha_baja_inciso.strftime("%d/%m/%Y"))
                else:
                    fechas_baja.append('')
            
            if c.contributory:
                contributoria.append('Si')
            else:
                contributoria.append('No')
            rfccve.append(acc.first().personal.rfc_cve)
            rfchomocve.append(acc.first().personal.rfc_homocve)
            dom_cp.append(acc.first().personal.dom_cp)
            dom_poblacion.append(acc.first().personal.dom_poblacion)
            dom_estado.append(acc.first().personal.dom_estado)
            dom_callenum.append(acc.first().personal.dom_callenum)
            dom_colonia.append(acc.first().personal.dom_colonia)
            ivas.append(acc.first().personal.iva)
            recargos.append(acc.first().personal.rpf)
            derechos.append(acc.first().personal.derecho)
            primas_neta.append(acc.first().personal.p_neta)
            primas_total.append(acc.first().personal.p_total)


            marcas.append('')
            modelos.append('')
            anios.append('')
            versiones.append('')
            series.append('')
            motores.append('')
            drivers.append('')
            adjuntos.append(adj)

            for rel in Relationship.objects.filter(accident = acc.first()):                           
                foliint.append(rel.id if rel else '')
                fechacre.append(rel.created_at.strftime("%d/%m/%Y") if rel and rel.created_at else '')
                if c.parent and c.parent.parent and c.parent.parent.parent.celula:
                    celula_poliza.append(c.parent.parent.parent.celula.celula_name)
                else:
                    celula_poliza.append("Sin célula")
                
                
                try:
                    subgrupoparent.append(c.parent.parent.name)
                    categoriaparent.append(c.parent.name)
                except:
                    subgrupoparent.append('')
                    categoriaparent.append('')
                observaciones.append('')
                ids.append('')

                try:
                    estatus_poliza.append( c.parent.parent.parent.get_status_display() )
                except:
                    estatus_poliza.append( "" )
                try:
                    statusCertsss.append( c.get_status_display() )
                except:
                    statusCertsss.append( "" )                    
                
                if c.contributory:
                    contributoria.append('Si')
                else:
                    contributoria.append('No')
                rfccve.append(rel.rfc_cve)
                rfchomocve.append(rel.rfc_homocve)
                dom_cp.append(rel.dom_cp)
                dom_poblacion.append(rel.dom_poblacion)
                dom_estado.append(rel.dom_estado)
                dom_callenum.append(rel.dom_callenum)
                dom_colonia.append(rel.dom_colonia)

                poliza_number.append(caratula.poliza_number)
                if caratula.contractor:
                    contratantes.append(caratula.contractor.full_name)
                else:
                    contratantes.append('Sin Contratante')
                cerficate_number.append(rel.certificate)
                if rel.is_active:
                    status.append('Activo')
                else:
                    status.append('Inactivo')

                if rel.is_active ==False:
                    ivasNDev.append(rel.iva_earned)
                    recargosNDev.append(rel.rpf_earned)
                    derechosNDev.append(rel.derecho_earned)
                    primas_netaNDev.append(rel.p_neta_earned)
                    primas_totalNDev.append(rel.p_total_earned)
                else:
                    ivasNDev.append(0)
                    recargosNDev.append(0)
                    derechosNDev.append(0)
                    primas_netaNDev.append(0)
                    primas_totalNDev.append(0)

                if rel.discharge_date:
                    fechas_baja.append(rel.discharge_date.strftime("%d/%m/%Y"))
                else:
                    if c.fecha_baja_inciso:
                        fechas_baja.append(c.fecha_baja_inciso.strftime("%d/%m/%Y"))
                    else:
                        fechas_baja.append('')

                if rel.birthdate:
                    fechas_nacimiento.append(rel.birthdate.strftime("%d/%m/%Y"))
                else:
                    fechas_nacimiento.append('')
                if rel.charge_date:
                    fechaAlta.append(rel.charge_date.strftime("%d/%m/%Y"))
                else:
                    if c.charge_date:
                        fechaAlta.append(c.charge_date.strftime("%d/%m/%Y"))    
                    else:
                        fechaAlta.append('')    

                alta_endoso = EndorsementCert.objects.filter(relationship = rel, org_name = org, endorsement__endorsement_type = 'A', endorsement__status =2).order_by('-endorsement_id')
                if alta_endoso.exists():
                    endosos_alta.append(alta_endoso.first().endorsement.number_endorsement)                                    
                else:
                    endosos_alta.append('')

                baja_endoso = EndorsementCert.objects.filter(relationship = rel, org_name = org, endorsement__endorsement_type = 'D', endorsement__status =2).order_by('-endorsement_id')
                if baja_endoso.exists():
                    endosos_baja.append(baja_endoso.first().endorsement.number_endorsement)
                else:
                    endosos_baja.append('')


                ivas.append(rel.iva)
                recargos.append(rel.rpf)
                derechos.append(rel.derecho)
                primas_neta.append(rel.p_neta)
                primas_total.append(rel.p_total)


                if c.start_of_validity:
                    vigencia_inicial.append(c.start_of_validity.strftime("%d/%m/%Y"))
                else:
                    vigencia_inicial.append(c.start_of_validity)
                if c.end_of_validity:
                    vigencia_final.append(c.end_of_validity.strftime("%d/%m/%Y"))
                else:
                    vigencia_final.append(c.end_of_validity)
                if caratula.start_of_validity:
                    vigipoliza.append(caratula.start_of_validity.strftime("%d/%m/%Y"))
                else:
                    vigipoliza.append(caratula.start_of_validity)
                if caratula.end_of_validity:
                    vigfpoliza.append(caratula.end_of_validity.strftime("%d/%m/%Y"))
                else:
                    vigfpoliza.append(caratula.end_of_validity) 

                ramo_name.append(caratula.ramo.ramo_name)
                subramo_name.append(caratula.subramo.subramo_name)
                aseguradora_alias.append(caratula.aseguradora.alias)
                if caratula.paquete:
                    package_name.append(caratula.paquete.package_name)
                else:
                    package_name.append('No especificado')
            


                tipos.append('Dependiente')
                
                nombres.append(rel.full_name)
                correos.append('')
                if rel.antiguedad:
                    fechas_de_antiguedad.append(rel.antiguedad.strftime("%d/%m/%Y"))
                else:
                    fechas_de_antiguedad.append(rel.antiguedad)

                if rel.sex == 'M':
                    sexos.append('Masculino')
                elif rel.sex == 'F':
                    sexos.append('Femenino')
                else:
                    sexos.append('No definido')

                marcas.append('')
                modelos.append('')
                anios.append('')
                versiones.append('')
                series.append('')
                motores.append('')
                drivers.append('')
                adjuntos.append(adj)

        elif autos.exists():
            tipos.append('Automóviles')
            marcas.append(autos.first().brand  )
            modelos.append(autos.first().model  )
            anios.append(autos.first().year  )
            versiones.append(autos.first().version  )
            series.append(autos.first().serial  )
            motores.append(autos.first().engine)
            drivers.append(autos.first().driver)
            adjuntos.append(adj)
            contributoria.append('No APlica')
            rfccve.append('')
            rfchomocve.append('')
            dom_cp.append('')
            dom_poblacion.append('')
            dom_estado.append('')
            dom_callenum.append('')
            dom_colonia.append('')
            foliint.append(autos.first().id if autos.first() else '')
            fechacre.append(autos.first().created_at.strftime("%d/%m/%Y") if autos.first() and autos.first().created_at else '')
            if c.certificado_inciso_activo ==False:
                ivasNDev.append(c.iva_earned)
                recargosNDev.append(c.rpf_earned)
                derechosNDev.append(c.derecho_earned)
                primas_netaNDev.append(c.p_neta_earned)
                primas_totalNDev.append(c.p_total_earned)
            else:
                ivasNDev.append(0)
                recargosNDev.append(0)
                derechosNDev.append(0)
                primas_netaNDev.append(0)
                primas_totalNDev.append(0)

            if c.fecha_baja_inciso:
                fechas_baja.append(c.fecha_baja_inciso.strftime("%d/%m/%Y"))
            else:
                fechas_baja.append('')

            ivas.append(c.iva)
            recargos.append(c.rpf)
            derechos.append(c.derecho)
            primas_neta.append(c.p_neta)
            primas_total.append(c.p_total)
            
            nombres.append('')
            sexos.append('')
            if permiso_correos == True or permiso_correos == 'True':
                correos.append(autos.first().email)
            else:
                correos.append('')
            fechas_de_antiguedad.append('')
            fechas_nacimiento.append('')
        elif danioss.exists():
            tipos.append('Daños')
            marcas.append(danioss.first().item_details )
            modelos.append('' )
            anios.append('')
            versiones.append('')
            series.append(''  )
            motores.append('')
            drivers.append('')
            adjuntos.append(adj)
            contributoria.append('No Aplica')
            rfccve.append('')
            rfchomocve.append('')
            dom_cp.append('')
            dom_poblacion.append('')
            dom_estado.append('')
            dom_callenum.append('')
            dom_colonia.append('')
            foliint.append(danioss.first().id if danioss.first() else '')
            fechacre.append(danioss.first().created_at.strftime("%d/%m/%Y") if danioss.first() and danioss.first().created_at else '')
            if c.certificado_inciso_activo ==False:
                ivasNDev.append(c.iva_earned)
                recargosNDev.append(c.rpf_earned)
                derechosNDev.append(c.derecho_earned)
                primas_netaNDev.append(c.p_neta_earned)
                primas_totalNDev.append(c.p_total_earned)
            else:
                ivasNDev.append(0)
                recargosNDev.append(0)
                derechosNDev.append(0)
                primas_netaNDev.append(0)
                primas_totalNDev.append(0)

            if c.fecha_baja_inciso:
                fechas_baja.append(c.fecha_baja_inciso.strftime("%d/%m/%Y"))
            else:
                fechas_baja.append('')

            ivas.append(c.iva)
            recargos.append(c.rpf)
            derechos.append(c.derecho)
            primas_neta.append(c.p_neta)
            primas_total.append(c.p_total)
            
            nombres.append('')
            sexos.append('')
            if permiso_correos == True or permiso_correos == 'True':
                correos.append(danioss.first().email)
            else:
                correos.append('')
            fechas_de_antiguedad.append('')
            fechas_nacimiento.append('')
        else:
            foliint.append('')
            fechacre.append('')
            tipos.append('No especificado')
            nombres.append('No especificado')
            correos.append('No especificado')
            fechas_de_antiguedad.append('No especificado')
            sexos.append('No especificado')
            marcas.append('')
            modelos.append('')
            anios.append('')
            versiones.append('')
            series.append('')
            motores.append('')
            drivers.append('')
            ivas.append('')
            recargos.append('')
            derechos.append('')
            primas_neta.append('')
            primas_total.append('')
            
            if c.fecha_baja_inciso:
                fechas_baja.append(c.fecha_baja_inciso.strftime("%d/%m/%Y"))
            else:
                fechas_baja.append('')

            ivasNDev.append(0)
            recargosNDev.append(0)
            derechosNDev.append(0)
            primas_netaNDev.append(0)
            primas_totalNDev.append(0)            
            fechas_nacimiento.append('')
            adjuntos.append(adj)
            if c.contributory:
                contributoria.append('Si')
            else:
                contributoria.append('No')
            rfccve.append('')
            rfchomocve.append('')
            dom_cp.append('')
            dom_poblacion.append('')
            dom_estado.append('')
            dom_callenum.append('')
            dom_colonia.append('')

    # dataframe columns
    campo_celula, campo_agrupacion, campo_lineanegocio, moduleName = getOrgInfo(org)
    nombreModulo = 'Célula póliza grupo'
    df = pd.DataFrame({
        "No poliza": poliza_number,
        "Contratante": contratantes,
        "Certificado":  cerficate_number,
        "Estatus":  status,
        "Ramo": ramo_name,
        "Subramo": subramo_name,
        "Aseguradora": aseguradora_alias,
        "Paquete": package_name,
        "Tipo": tipos ,
        "Nombre del Asegurado": nombres,
        "Sexo": sexos,
        "Correo" : correos, 
        "Marca" : marcas,
        "Modelo" : modelos,
        "Año" : anios,
        "Version": versiones,
        "Serie" : series,
        "Motor"  :motores,
        "Conductor"  : drivers,
        "Vigencia inicial Póliza" :vigipoliza ,
        "Vigencia final Póliza":vigfpoliza ,
        "Vigencia inicial Certificado" :vigencia_inicial ,
        "Vigencia final Certificado":vigencia_final ,
        "Fecha de antiguedad": fechas_de_antiguedad,
        "Prima neta": primas_neta,
        "Derechos": derechos,
        "Recargos": recargos,
        "Iva": ivas ,
        "Prima total": primas_total,
        "Observaciones": observaciones,

        # "Estatus Certificado": statusCertsss,
        "Estatus póliza grupo": estatus_poliza,
        moduleName if moduleName else nombreModulo: celula_poliza,
        "Prima neta No Devengadas": ["-%s"%i if i and i != '' and i  != 0 else i for i in primas_netaNDev],
        "Derechos No Devengadas": ["-%s"%i if i and i != '' and i  != 0 else i for i in derechosNDev],
        "Recargos No Devengadas": ["-%s"%i if i and i != '' and i  != 0 else i for i in recargosNDev],
        "Iva No Devengadas": ["-%s"%i  if i and i != '' and i  != 0 else i for i in ivasNDev] ,
        "Prima total No Devengadas": ["-%s"%i if i and i != '' and i  != 0 else i for i in primas_totalNDev],
        
        "Fecha de nacimiento": fechas_nacimiento,
        "Fecha alta":  fechaAlta,
        "Fecha de baja": fechas_baja,
        "Endoso de baja": endosos_baja,
        "Endoso de alta": endosos_alta,
        "Adjuntos": adjuntos,
        "Contributorio": contributoria,
        'RFC Cve': rfccve,
        'RFC HomoClave': rfchomocve,
        'Domicilio Calle-Núm': dom_callenum,
        'Domicilio Colonia': dom_colonia,
        'Domicilio C.P.': dom_cp,
        'Domicilio Municipio/Población' : dom_poblacion,
        'Domicilio Estado': dom_estado,
        'Subgrupo': subgrupoparent,
        'Categoría': categoriaparent,
        "ID":ids,
        'Folio Interno':foliint,
        'Fecha Creación':fechacre,

    })
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pd.ExcelWriter(filename, engine='xlsxwriter')

    # Convert the dataframe to an XlsxWriter Excel object.
    df.to_excel(writer, sheet_name='Sheet1', index=False)

    # Close the Pandas Excel writer and output the Excel file.
    writer.save()
    try:
        gettingby = gettingby = post['gettingby']
        print('gettingby certs',gettingby)
        if gettingby:
            print('reporte ancora--certificados--',gettingby,len(certificados))
            if len(certificados) ==0:
                data = {}
                data['link_reporte'] = ''
                data['count'] = len(certificados)
                return data
            else:
                url_response = 'timeout'
                try:
                    with open(filename) as f:
                        url_response = upload_to_s3(filename, org)
                        try:
                            redis_client.publish(self.request_id, url_response)
                        except:
                            pass
                        print('url',url_response)
                        import os
                        os.remove(filename)        
                        data = {}
                        data['link_reporte'] = url_response
                        data['count'] = len(certificados)
                        return data
                except Exception as fd:
                    print('fd--error up file--',fd)   
                    data = {}
                    data['link_reporte'] = url_response
                    data['count'] = len(certificados)
                    return data
        else:
            from core.utils import send_mail
            if email:
                message = '''
                Estimado usuario, le enviamos el reporte de certificados solicitado anteriormente, saludos.
                ''' 
                send_mail(email, filename, message)
            else:
                import os
                os.remove(filename)
            return 'Executed'
    except Exception as ers:
        print('error certs anc',ers)
        from core.utils import send_mail
        if email:
            message = '''
            Estimado usuario, le enviamos el reporte de certificados solicitado anteriormente, saludos.
            ''' 
            send_mail(email, filename, message)
        else:
            import os
            os.remove(filename)

        return 'Executed'
        os.remove(filename)
        return 'ReportePolizas Excel Executed'
        

#AIA
redis_client = redis.Redis(host='localhost', port=6379, db=0)
@app.task(bind=True, queue=settings.QUEUE)
def renovaciones_reportaia_task(self, post):
    org = post['org']    
    report_by = int(post['report_by']   )
    since = post['since']
    until = post['until']

    fp = [12,24,6,5,4,3,2,1,7,14,15]

    # Filtro de status
    st = [1,2,4,10,11,12,13,14,15]
    
    polizas = Polizas.objects.filter(status__in = st, 
                                     forma_de_pago__in = fp, 
                                     org_name=post['org'],)
    # -----------------------------***********************************************-----------------------------------

    # Filtro de fechas
    try:
        f = "%d/%m/%Y"        
        since = datetime.strptime(since , f)
        until = datetime.strptime(until , f)

    except:
        f = "%m/%d/%Y"        
        since = datetime.strptime(since , f)
        until = datetime.strptime(until , f)

    # Filtro de renovadas
    if report_by == 1:
        polizas = polizas.filter(renewed_status=1)
        date_filters = [Q(start_of_validity__gte=since),Q(start_of_validity__lte = until), Q(renewed_status = 1)]
        polizas = polizas.filter(reduce(operator.and_, date_filters), document_type__in = list([1,3,11,12]) ,org_name = post['org']).exclude(status__in = [1,2])
    elif report_by == 2:
        polizas = polizas.filter(renewed_status=0)
        date_filters = [Q(start_of_validity__gte=since),Q(start_of_validity__lte = until), Q(status__in = [10, 13,14]), Q(renewed_status = 0)]
        polizas = polizas.filter(reduce(operator.and_, date_filters), document_type__in = list([1,3,11,12]) ,org_name = post['org']).exclude(status__in = [1,2])
    else:
        polizas = polizas   

    polizas = polizas.filter(document_type__in = list([1,3,11,12]) ,org_name = post['org']).exclude(status__in = [1,2])    
    try:
        # dataToFilter = {}
        print('post[user]', post['user'],  post['org'])
        dataToFilter = getDataForPerfilRestricted(post['user'], post['org'])
    except Exception as er:
        dataToFilter = {}
    if dataToFilter:
        # Contratantes***
        polizasCl = Polizas.objects.filter(document_type__in = [1,3,11,7,8,12,6,4], org_name = post['org'])
        polizasToF = Polizas.objects.filter(document_type__in = [1,3,11,7,8], org_name = post['org'])
        if dataToFilter['ccpr']:
            polizasToF = polizasToF.filter(contractor__in = list(dataToFilter['ccpr']))
        if dataToFilter['cgpr']:
            polizasToF = polizasToF.filter(contractor__group__in = list(dataToFilter['cgpr']))
        if dataToFilter['ccepr']:
            polizasToF = polizasToF.filter(contractor__cellule__in = list(dataToFilter['ccepr']))
        if dataToFilter['crpr']:
            polizasToF = polizasToF.filter(contractor__vendor__in = list(dataToFilter['crpr']))
        if dataToFilter['cspr']:
            polizasToF = polizasToF.filter(contractor__sucursal__in = list(dataToFilter['cspr']))
        # Pólizas ****
        if dataToFilter['pppr']:
            polizasToF = polizasToF.filter(pk__in = list(dataToFilter['pppr']))
        if dataToFilter['pgpr']:
            polizasToF = polizasToF.filter(contractor__group__in = list(dataToFilter['pgpr']))
        if dataToFilter['pcepr']:
            polizasToF = polizasToF.filter(celula__in = list(dataToFilter['pcepr']))
        if dataToFilter['prpr']:
            polizasToF = polizasToF.filter(ref_policy__referenciador__in = list(dataToFilter['prpr']))
        if dataToFilter['pspr']:
            polizasToF = polizasToF.filter(sucursal__in = list(dataToFilter['pspr']))
        if dataToFilter['papr']:
            polizasToF = polizasToF.filter(groupinglevel__in = list(dataToFilter['papr']))
        if dataToFilter['pcapr']:
            polizasToF = polizasToF.filter(clave__in = list(dataToFilter['pcapr']))
        if dataToFilter['psrpr']:
            polizasToF = polizasToF.filter(subramo__subramo_code__in = list(dataToFilter['psrpr']))
        if dataToFilter['paspr']:
            polizasToF = polizasToF.filter(aseguradora__in = list(dataToFilter['paspr']))
        if dataToFilter['pstpr']:
            polizasToF = polizasToF.filter(status__in = list(dataToFilter['pstpr']))
        
        polizasCT = polizasCl.filter(document_type = 12, parent__in = list(polizasToF))
        polizasGT = polizasCl.filter(document_type = 6, parent__parent__parent__in = list(polizasToF))
        polizasFin = list(polizasToF.values_list('pk', flat = True)) + list(polizasCT.values_list('pk', flat = True)) + list(polizasGT.values_list('pk', flat = True))
        polizas = polizas.filter(pk__in = list(polizasFin)) 
    # ········ Inicia Excel ········  
    # Empieza insertado de imagen
    try:
        info_org = getInfoOrg(post)
        if len(info_org['logo']) != 0:
            archivo_imagen = 'https://miurabox.s3.amazonaws.com/cas/' + info_org['logo']
        else:
            archivo_imagen = 'saam.jpg'
    except Exception as g:
        archivo_imagen = 'saam.jpg'
    tipo_reporte = "Reporte de pólizas del " + str(since.strftime("%d/%m/%y")) + " al " + str(until.strftime("%d/%m/%y"))
    row_num = 10 # A partir de aqui pinta la tabla de resultados
    columns = ['Tipo', 'No.Póliza', 'Contratante','Grupo', 'Email', 'Teléfono', 'Proveedor', 'Subramo',
            'Forma de Pago', 'Estatus', 'Inicio Póliza', 'Fin Póliza', 'Moneda', 'Prima Neta', 'Descuento',
            'RPF', 'Derecho', 'IVA', 'Prima Total', 'Comisión','Comisión Porcentaje', 'Clave', 'Sucursal', 'Referenciador', 'Creado por',
            'Fecha creación', 'Observaciones', 'Asegurado', 'Marca', 'Modelo', 'Versión', 'Año',
            'Placas', 'Motor', 'Adaptaciones', 'Conductor','Dirección','Código Postal','Identificador','Ejecutivo Cobranza',
            'Responsable','Ramo','Cancelacion','Renovable','OT/Póliza asociada','Origen Póliza','Paquete', 'Subgrupo','Subsubgrupo',
            'Agrupación','Subagrupación','Subsubagrupación','Clasificación','Línea Negocio','Célula','Fecha Cancelación','Adjuntos','Colectividad/PG completa']
            
    try:
        columns =ast.literal_eval(post['cols1'])
    except:
        pass        
    # ******************************************
    # Empieza insertado de imagen


    columns.insert(3,'Email')
    columns.append('ID_SAAM')
    try:
        info_org = getInfoOrg(post)
        if len(info_org['logo']) != 0:
          archivo_imagen = 'https://miurabox.s3.amazonaws.com/cas/' + info_org['logo']
        else:
          archivo_imagen = 'saam.jpg'
    except Exception as d:
        archivo_imagen = 'saam.jpg'
    tipo_reporte = "Reporte de pólizas del " + str(since.strftime("%d/%m/%y")) + " al " + str(until.strftime("%d/%m/%y"))
       
    asegurado = []
    antiguedad = []
    referenciador = []
    creadopor = []
    dateRen = []
    otRen = []
    responsable = []
    contratante = []
    contratanteE = []
    contratanteP = []
    contratanteG = []
    collection = []
    clave = []
    cp = []
    caratulas = []
    paquete = []
    valueMA = []  
    valueModA = [] 
    valueVA = [] 
    valueYA = [] 
    valueLPA = [] 
    valueEA = [] 
    valueAdA = [] 
    valueDrA = []
    tipo_admin = []
    nueva = []
    nivelAg = []
    grupo3 = []
    grupo4 = []
    clasifica = []
    poliza_siguiente = []
    poliza_anterior = []
    businessLine = []

    contactos_nombre = []
    contactos_email = []
    contactos_telefono = []
    celulaC = []
    cars_serials = []
    grupo1 = []       
    grupo2 = []
    fechac = []     
    adjuntos = []  
    comisionPercent = []  
    completaPGC = []          
    edocir = []
    descuento = []
    correo_contratante = []
    ids = []
    polizas = polizas.order_by('id')
    bitacora = []
    # polizas = polizas.order_by('natural__full_name','juridical__j_name')
    
    for r in polizas: 
        comment = Comments.objects.filter(
            model = 1, 
            id_model = r.id,
            is_child = False
        )
        if comment.exists():
            comment = comment.last()
            bitacora.append(comment.content)
        else:
            bitacora.append('')

        if r.contractor.email:
            correo_contratante.append(r.contractor.email)
        else:
            correo_contratante.append('')
        ids.append(r.id)
        completa = ''
        comp = r.comision_percent
        adj = 'No'
        if PolizasFile.objects.filter(owner__id = r.id, org_name= r.org_name).exists():
            adj = 'Si'
        else:
            adj  ='No'

        if r.state_circulation:
            valstate = r.state_circulation
            try:
                valstate = getStates(int(r.state_circulation))
            except:
                valstate = r.state_circulation
        else:
            valstate = ''
        anterior = OldPolicies.objects.filter(new_policy = r)
        posterior = OldPolicies.objects.filter(base_policy = r)
        fec = ''
        if r.status ==11:
          try:
            fec = r.date_cancel.strftime("%d/%m/%Y") 
          except:
            fec = ''
        else:
          fec = ''
        if anterior.exists():
          anterior = anterior.last()
          poliza_anterior.append(anterior.base_policy.poliza_number)
        else:
          poliza_anterior.append(0)

        if posterior.exists():
          posterior = posterior.last()
          try:
            poliza_siguiente.append(posterior.new_policy.poliza_number)
          except:
            poliza_siguiente.append(0)
        else:
          poliza_siguiente.append(0)
        if r.document_type ==3 or r.document_type ==11:
            completa = 'Si'
            if r.document_type ==3:
                if (r.poliza_number ==None or r.poliza_number=='') or (r.receipts_by==1 and Recibos.objects.filter(poliza__id = r.id, receipt_type =1).exclude(status = 0).count() ==0):
                    completa = 'No'
                if Polizas.objects.filter(parent__id = r.id, document_type =4).count() ==0:
                    completa = 'No'
                if Polizas.objects.filter(parent__parent__id = r.id, document_type =5).count() ==0:
                    completa = 'No'
                if Polizas.objects.filter(parent__parent__parent__id = r.id, document_type =6).count() ==0:
                    completa = 'No'
                if Package.objects.filter(policy = r.id, type_package =2).count() ==0:
                    completa = 'No'
                if (r.receipts_by==2 and Polizas.objects.filter(parent__id = r.id, document_type =4).exists()):
                    if Recibos.objects.filter(poliza__in = list(Polizas.objects.filter(parent__id = r.id, document_type =4)), receipt_type =1).exclude(status = 0).count() ==0:
                        completa = 'No'
                if (r.receipts_by==3 and Polizas.objects.filter(parent__parent__parent__id = r.id, document_type =6).exists()):
                    if Recibos.objects.filter(poliza__in = list(Polizas.objects.filter(parent__parent__parent__id = r.id, document_type =6)), receipt_type =1).exclude(status = 0).count() ==0:
                        completa = 'No'
            if r.document_type ==11:
                if (r.poliza_number ==None or r.poliza_number==''):
                    completa = 'No'
                if Polizas.objects.filter(parent__id = r.id, document_type =12).count() ==0:
                    completa = 'No'
        if r.document_type == 12:

            try:
                pn = r.parent.poliza_number
            except:
                pn = ''
            caratulas.append(pn)
            if r.date_cancel:
              if r.status ==11:
                try:
                  fec = r.date_cancel.strftime("%d/%m/%Y") 
                except:
                  fec = ''
              else:
                fec = ''
            else:
              if r.status ==11:
                try:
                  fec = r.parent.date_cancel.strftime("%d/%m/%Y") 
                except:
                  fec = ''
              else:
                fec = ''
        else:
            caratulas.append('')
        if r.document_type == 3:
            tipo_admin.append(r.get_administration_type_display()) 
        else:
            tipo_admin.append('')
        nuevaX = '' 
   
        if r.address:
           pc = r.address.postal_code
        else:
           pc = ''
        antig = get_antiguedad(r.start_of_validity)
        if r.owner:
            val_owner = r.owner.first_name + ' '+ str(r.owner.last_name)
        else:
            val_owner = ''
        if r.paquete:
            pac = r.paquete.package_name
        else:
            pac = ''
        if r.responsable:
            val_resp = r.responsable.first_name + ' '+ str(r.responsable.last_name)
        else:
            val_resp = ''
        if r.collection_executive:
            val_col = r.collection_executive.first_name + ' '+ str(r.collection_executive.last_name)
        else:
            val_col = ''
        if r.clave:
            try:
                cve = r.clave.name + ' '+ str(r.clave.clave)
                cve = ((((((cve).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
            except Exception as ere:
                cve = ''
        else:
            cve = ''
        # if not r.document_type == 11:
        if r.business_line:
            if int(r.business_line) ==1:
                businessLine_ = 'Comercial'
            elif int(r.business_line) ==2:
                businessLine_ = 'Personal'
            elif int(r.business_line) ==0:
                businessLine_ = 'Otro'
            else:
                try:
                    if int(r.business_line) ==0:
                        businessLine_ = 'Otro'
                    else:
                        businessLine_ = ''
                except:
                    businessLine_ = ''
        else:
            businessLine_ = ''
        if r.contractor:
            contratan = ((((((r.contractor.full_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
            contratanE = r.contractor.email
            contratanP = r.contractor.phone_number
            # contratanG = r.contractor.group.group_name
            if r.contractor.classification:
                clasifica_ = r.contractor.classification.classification_name  
            else:
                clasifica_='-----'
            if r.celula:
                cel = r.celula.celula_name  
            else:
                cel='-----'
            contratanG = ((((((r.contractor.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
            try:
                if r.contractor.group.type_group == 1:
                    contratanG = ((((((r.contractor.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    grupo1_ = ''
                    grupo2_ = ''
                elif r.contractor.group.type_group == 2:
                    grupotype1 = Group.objects.get(pk = r.contractor.group.parent.id)
                    contratanG = ((((((grupotype1.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    grupo1_ = ((((((r.contractor.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    grupo2_ = ''
                elif r.contractor.group.type_group == 3:
                    grupotype1 = Group.objects.get(pk = r.contractor.group.parent.parent.id)
                    contratanG = ((((((grupotype1.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    subgrupotype2 = Group.objects.get(pk = r.contractor.group.parent.id)
                    grupo1_ = ((((((subgrupotype2.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    grupo2_ = ((((((r.contractor.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
            except Exception as e:
                contratanG = ''
                grupo1_ = ''
                grupo2_ = ''
            try:
                if r.groupinglevel:
                    if r.groupinglevel.type_grouping == 1:
                        nivelAg_ = ((((((r.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo3_ = ''
                        grupo4_ = ''
                    elif r.groupinglevel.type_grouping == 2:
                        grupotype1 = GroupingLevel.objects.get(pk = r.groupinglevel.parent.id)
                        nivelAg_ = ((((((grupotype1.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo3_ = ((((((r.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo4_ = ''
                    elif r.groupinglevel.type_grouping == 3:
                        grupotype1 = GroupingLevel.objects.get(pk = r.groupinglevel.parent.parent.id)
                        nivelAg_ = ((((((grupotype1.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        subgrupotype2 = GroupingLevel.objects.get(pk = r.groupinglevel.parent.id)
                        grupo3_ = ((((((subgrupotype2.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo4_ = ((((((r.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                else:
                    nivelAg_ = ''
                    grupo3_ = ''
                    grupo4_ = ''  
            except Exception as e:
                nivelAg_ = ''
                grupo3_ = ''
                grupo4_ = ''

            contacto = ContactInfo.objects.filter(contractor=r.contractor)

            if contacto:
                contacto_nombre = contacto[0].name
                contacto_telefono = contacto[0].phone_number
                contacto_email = contacto[0].email
            else:
                contacto_nombre = ''
                contacto_telefono = ''
                contacto_email = ''

        else:
            contratan = ''
            contratanE = ''
            contratanP = ''
            clasifica_='-----'                
            cel='-----'
            contratanG = ''                
            grupo1_ = ''
            grupo2_ = ''
            nivelAg_ = ''
            grupo3_ = ''
            grupo4_ = '' 
            contacto_nombre = ''
            contacto_telefono = ''
            contacto_email = ''

        try:
            old = OldPolicies.objects.filter(base_policy__id = r.id)
            try:
              date_renovacion = old[0].created_at.strftime("%d/%m/%Y")
            except Exception as rr:
                date_renovacion = 'Por renovar'
            try:
                ot_renovacion = old[0].new_policy.poliza_number if old[0].new_policy.poliza_number else old[0].new_policy.internal_number
            except Exception as rr:
                ot_renovacion = 'Por renovar'
        except Exception as dwe:
            date_renovacion = 'Por renovar'
            ot_renovacion = 'Por renovar'
        try:
            refs_policy = ReferenciadoresInvolved.objects.filter(policy = r.id,is_changed=False).values_list('referenciador__first_name','referenciador__last_name')
            if len(refs_policy):
                v = []
                try:
                    for it in refs_policy:
                        v.append(it[0] +' '+ str(it[1])+str('; '))
                    referenc = v
                except Exception as y:
                    referenc = []
            else:
                referenc = []
        except Exception as e:
            referenc = []

        car_serial = ''
        
        if r.ramo.ramo_code == 1:
            form = Life.objects.filter(policy = r.id)
            if form:
                try:
                    if form[0].personal:
                        value = form[0].personal.full_name
                    else:
                        value = ''
                except Exception as e:
                    value = ''
                valueM = '' 
                valueMod = '' 
                valueV = '' 
                valueY = ''
                valueLP = '' 
                valueE = '' 
                valueAd = '' 
                valueDr = ''
            else:
                value = ''
                valueM = '' 
                valueMod = '' 
                valueV = '' 
                valueY = ''
                valueLP = '' 
                valueE = '' 
                valueAd = '' 
                valueDr = ''
        elif r.ramo.ramo_code == 2:
            form = AccidentsDiseases.objects.filter(policy = r.id)
            if form:
                value = form[0].personal.full_name
                valueM = '' 
                valueMod = '' 
                valueV = '' 
                valueY = ''
                valueLP = '' 
                valueE = '' 
                valueAd = '' 
                valueDr = ''
            else:
                value = ''
                valueM = '' 
                valueMod = '' 
                valueV = '' 
                valueY = ''
                valueLP = '' 
                valueE = '' 
                valueAd = '' 
                valueDr = ''
        elif r.subramo.subramo_code == 9:
            form = AutomobilesDamages.objects.filter(policy = r.id)
            if form:
                try:
                    form[0].model = ((((((form[0].model).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    form[0].version = ((((((form[0].version).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                except Exception as ers:
                    form[0].model = form[0].model
                    form[0].version = form[0].version

                value = str(form[0].brand) + '-' + str(form[0].model )+ '-' + str(form[0].version)
                valueM = form[0].brand 
                valueMod = form[0].model 
                valueV = form[0].version 
                valueY = form[0].year 
                valueLP = form[0].license_plates 
                valueE = form[0].engine 
                valueAd = form[0].adjustment 
                valueDr = form[0].driver

                car_serial = form[0].serial 
            else:
                value = ''
                valueM = '' 
                valueMod = '' 
                valueV = '' 
                valueY = ''
                valueLP = '' 
                valueE = '' 
                valueAd = '' 
                valueDr = ''
        elif r.ramo.ramo_code == 3 and not r.subramo.subramo_code == 9:
            form = Damages.objects.filter(policy = r.id)
            if form:
                value = form[0].insured_item
                valueM = '' 
                valueMod = '' 
                valueV = '' 
                valueY = ''
                valueLP = '' 
                valueE = '' 
                valueAd = '' 
                valueDr = ''
            else:
                value = ''
                valueM = '' 
                valueMod = '' 
                valueV = '' 
                valueY = ''
                valueLP = '' 
                valueE = '' 
                valueAd = '' 
                valueDr = ''
        if value:
            value = ((((((value).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'') 
        else:
            value = value
        
        nueva.append('')
        if r.document_type ==12:
            try:
                if r.parent.celula:
                    cel = r.parent.celula.celula_name
                else:
                    cel = '-'
                if r.parent.groupinglevel:
                    if r.parent.groupinglevel.type_grouping == 1:
                        nivelAg_ = ((((((r.parent.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo3_ = ''
                        grupo4_ = ''
                    elif r.parent.groupinglevel.type_grouping == 2:
                        grupotype1 = GroupingLevel.objects.get(pk = r.parent.groupinglevel.parent.id)
                        nivelAg_ = ((((((grupotype1.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo3_ = ((((((r.parent.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo4_ = ''
                    elif r.parent.groupinglevel.type_grouping == 3:
                        grupotype1 = GroupingLevel.objects.get(pk = r.parent.groupinglevel.parent.parent.id)
                        nivelAg_ = ((((((grupotype1.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        subgrupotype2 = GroupingLevel.objects.get(pk = r.parent.groupinglevel.parent.id)
                        grupo3_ = ((((((subgrupotype2.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo4_ = ((((((r.parent.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                else:
                    nivelAg_ = ''
                    grupo3_ = ''
                    grupo4_ = ''  
            except Exception as e:
                nivelAg_ = ''
                grupo3_ = ''
                grupo4_ = ''
        else:
            if r.celula:
                cel = r.celula.celula_name
            else:
                cel = '-'
        asegurado.append(value)
        if r.document_type ==3 or r.document_type ==11:
            completaPGC.append(completa)
        else:
            completaPGC.append('')
        dateRen.append(date_renovacion)
        otRen.append(ot_renovacion)
        referenciador.append(referenc)
        antiguedad.append(antig)
        creadopor.append(val_owner)
        contratante.append(contratan)
        contratanteE.append(contratanE)
        contratanteP.append(contratanP)
        contratanteG.append(contratanG)
        responsable.append(val_resp)
        collection.append(val_col)
        paquete.append(pac)
        clave.append(cve)
        cp.append(pc)
        valueMA.append(valueM) 
        valueModA.append(valueMod)  
        valueVA.append(valueV) 
        valueYA.append(valueY) 
        valueLPA.append(valueLP)  
        valueEA.append(valueE)  
        valueAdA.append(valueAd)  
        valueDrA.append(valueDr) 
        # ---
        nivelAg.append(nivelAg_)
        grupo3.append(grupo3_)
        grupo4.append(grupo4_)
        clasifica.append(clasifica_)
        celulaC.append(cel)
        businessLine.append(businessLine_)

        contactos_nombre.append(contacto_nombre)
        contactos_email.append(contacto_email)
        contactos_telefono.append(contacto_telefono)
        cars_serials.append(car_serial)
        grupo1.append(grupo1_)
        grupo2.append(grupo2_)            
        fechac.append(fec)            
        adjuntos.append(adj)            
        comisionPercent.append(comp)
        edocir.append(valstate)           
        # ----   
    df = pd.DataFrame({
        'Tipo'   : [checkDocumentType(x) for x in polizas.values_list('document_type', flat = True)],
        'No.Póliza'   : polizas.values_list('poliza_number', flat = True),
        'Contratante'   : contratante,
        'Email'   : contratanteE,
        'Grupo'   : contratanteG,
        'Proveedor'   : polizas.values_list('aseguradora__alias', flat = True),
        'Ramo'   : polizas.values_list('ramo__ramo_name', flat = True),
        'Subramo'   : polizas.values_list('subramo__subramo_name', flat = True),
        'Identificador'   : [(((((((x).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')) if x else '' for x in polizas.values_list('identifier', flat = True)],
        'Paquete': paquete,
        'Prima Neta': [ float(x.p_neta) if x.p_neta else 0 for x in polizas],
        'Descuento': list(map(float, polizas.values_list('descuento', flat = True))),
        'RPF': [ float(x.rpf) if x.rpf else 0 for x in polizas],
        'Derecho': [ float(x.derecho) if x.derecho else 0 for x in polizas],
        'IVA': [ float(x.iva) if x.iva else 0 for x in polizas],
        'Prima Total': [ float(x.p_total) if x.p_total else 0 for x in polizas],
        'Comisión': [ float(x.comision) if x.comision else 0 for x in polizas],
        'Comisión Porcentaje': comisionPercent,
        'Forma de Pago'   : [checkPayForm(x) for x in polizas.values_list('forma_de_pago', flat = True)],
        'Asegurado'   : asegurado,
        'Estatus'   : [ checkStatusPolicy(x) for x in  polizas.values_list('status', flat = True)],
        'Inicio Póliza'   : polizas.values_list('start_of_validity', flat = True) ,
        'Fin Póliza'   : polizas.values_list('end_of_validity', flat = True),
        'Referenciador'   : [str(x).replace('[','').replace(']','').replace('\'','').replace(';','') for x in referenciador],
        'Moneda' : [ checkCurrency(x) for x in polizas.values_list('f_currency', flat = True)],
        'Fecha creación'   : polizas.values_list('created_at', flat = True) ,
        'Observaciones'   : [(((((((x).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')) if x else '' for x in polizas.values_list('observations', flat = True)],
        'Creado por'   : creadopor,
        'Clave'   : clave,
        'Marca'   : valueMA,
        'Modelo'   : valueModA,
        'Año'   : valueYA,
        'Versión'   : valueVA,
        'Placas'   : valueLPA,
        'Motor'   : valueEA,
        'Adaptaciones'   : valueAdA,
        'Dirección': polizas.values_list('address__administrative_area_level_1', flat = True),
        'Ejecutivo Cobranza'   : collection,
        'Responsable'   : responsable,
        'Sucursal'   : polizas.values_list('sucursal__sucursal_name', flat = True),
        'Renovable'   : [checkRenewable(x) for x in polizas.values_list('is_renewable', flat = True)],
        'OT/Póliza asociada'   : otRen,
        'Origen Póliza'   : nueva,
        'Agrupación'   : nivelAg ,
        'Subagrupación'   : grupo3 ,
        'Subsubagrupación'   : grupo4 ,
        'Clasificación'   : clasifica ,
        'Línea de Negocio'   : businessLine ,
        'Célula'   : celulaC ,
        'Contacto(nombre)'   : contactos_nombre,
        'Contacto(telefono)'   : contactos_telefono,
        'Contacto(email)'   : contactos_email,
        'Carátula': caratulas,
        'Póliza Siguiente': poliza_siguiente,
        'Fecha Cancelación'   : fechac,
        'Adjuntos': adjuntos,
        'Colectividad/PG completa'   : completaPGC,
        'Estado Circulación'   : edocir, 
        'Tipo de Administración': tipo_admin,
        'ID_SAAM'   : polizas.values_list('id', flat = True),
        'Bitacora': bitacora
        # 'Póliza Anterior': poliza_anterior,
        # 'Teléfono'   : contratanteP,
        # 'OT Renovación'   : otRen,
        # 'Código Postal': cp,
        # 'Fecha renovación'   : dateRen,
        # 'Serie'   : cars_serials,
        # 'Conductor'   : valueDrA,
        # 'Motivo de Cancelación'   : polizas.values_list('reason_cancel', flat = True),
        # 'Subgrupo'   : grupo1 ,
        # 'Subsubgrupo'   : grupo2,



        # 'renewed_status': polizas.values_list('renewed_status', flat = True),
        # 'correo_contratante': correo_contratante,
        # 'ids': polizas.values_list('id', flat = True),
        # 'antiguedad'   : antiguedad,

        # 'tipo_reporte': tipo_reporte,
        # 'email_org':info_org['email'],
        # 'phone_org':info_org['phone'],
        # 'webpage_org':info_org['webpage'],
        # 'address_org':info_org['address'],
        # 'urlname_org':info_org['name'],
        } )
    
    try:
        df['Inicio Póliza'] = pd.to_datetime(df['Inicio Póliza'], errors='ignore', format='%D/%B/%Y').dt.date
    except Exception as e:
        df['Inicio Póliza'] = polizas.values_list('start_of_validity', flat = True) 

    try:
        df['Fin Póliza'] = pd.to_datetime(df['Fin Póliza'], errors='ignore', format='%D/%B/%Y').dt.date
    except Exception as e:
        df['Fin Póliza'] = polizas.values_list('end_of_validity', flat = True) 

    try:
        df['Fecha creación'] = pd.to_datetime(df['Fecha creación'], errors='ignore', format='%D/%B/%Y').dt.date
    except Exception as e:
        df['Fecha creación'] = polizas.values_list('created_at', flat = True) 


    showtime = str(strftime("%Y_%m_%d_%H_%M_%S", gmtime()))
    since = post.get('since').replace('/', '_').replace('-','_')
    until = post.get('until').replace('/', '_').replace('-','_')
    showtime = str(strftime("%Y_%m_%d_%H_%M_%S", gmtime()))
    if self.request.id !=None and self.request.id !='None':
        filename = 'reporte_polizasrens_%s_%s_%s_%s.xlsx'%(org, self.request.id, str(since).split('T')[0], str(until).split('T')[0])
    else:
        filename = 'reporte_polizasrens_%s_%s_%s_%s.xlsx'%(org, showtime, str(since).split('T')[0], str(until).split('T')[0])
    writer = pd.ExcelWriter(filename, engine='xlsxwriter')
    # Convert the dataframe to an XlsxWriter Excel object.
    df.to_excel(writer, sheet_name='Sheet1', index=False)
    # Close the Pandas Excel writer and output the Excel file.
    writer.save()
    url_response = 'timeout'
    with open(filename) as f:
        url_response = upload_to_s3(filename, org)
        redis_client.publish(self.request.id, url_response) 
        os.remove(filename)         
    data = {}
    data['link'] = url_response
    data['count'] = len(polizas)
    return data
    #return url_response

redis_client = redis.Redis(host='localhost', port=6379, db=0)
@app.task(bind=True, queue=settings.QUEUE)
def polizas_reportaia_task(self, post):
    org = post['org']    
    report_by = int(post['report_by'])#fechaingreso,vigencia
    renovadas = int(post['renovadas'])#0renovadas.1noren,2todas
    include_ot = int(post['include_ot'])#1incluir,2soloots,3no incluir
    since = post['since']
    until = post['until']

    fp = [12,24,6,5,4,3,2,1,7,14,15]

    # Filtro de status
    st = [1,2,4,10,11,12,13,14,15]
    
    polizas = Polizas.objects.filter(status__in = st, 
                                     forma_de_pago__in = fp, 
                                     org_name=post['org'],)

    # Filtro de fechas
    try:
        f = "%d/%m/%Y"        
        since = datetime.strptime(since , f)
        until = datetime.strptime(until , f)

    except:
        f = "%m/%d/%Y"        
        since = datetime.strptime(since , f)
        until = datetime.strptime(until , f)

    # Filtro de renovadas
    if int(report_by) == 2:
        date_filters = [Q(start_of_validity__gte=since),Q(start_of_validity__lte = until)]
    elif int(report_by) == 1:
        date_filters = [Q(created_at__gte=since),Q(created_at__lte = until), Q(migrated = False)]
    else:
        date_filters = [Q(start_of_validity__gte=since),Q(start_of_validity__lte = until)]

    if renovadas == 1:
        polizas = polizas.filter(renewed_status=1)
        polizas = polizas.filter(reduce(operator.and_, date_filters), document_type__in = list([1,3,11,12]) ,org_name = post['org'])
    elif renovadas == 2:
        polizas = polizas.filter(renewed_status=0)
        polizas = polizas.filter(reduce(operator.and_, date_filters), document_type__in = list([1,3,11,12]) ,org_name = post['org'])
    else:
        polizas = polizas   

    if include_ot ==1:
        polizas = polizas
    elif include_ot ==2:
        polizas = polizas.filter(document_type__in = list([1,3,11,12]) ,org_name = post['org'], status__in = [1,2])    
    elif include_ot ==3:
        polizas = polizas.filter(document_type__in = list([1,3,11,12]) ,org_name = post['org']).exclude(status__in = [1,2])   
    else:
        polizas = polizas 
    try:
        # dataToFilter = {}
        print('post[user]', post['user'],  post['org'])
        dataToFilter = getDataForPerfilRestricted(post['user'], post['org'])
    except Exception as er:
        dataToFilter = {}
    if dataToFilter:
        # Contratantes***
        polizasCl = Polizas.objects.filter(document_type__in = [1,3,11,7,8,12,6,4], org_name = post['org'])
        polizasToF = Polizas.objects.filter(document_type__in = [1,3,11,7,8], org_name = post['org'])
        if dataToFilter['ccpr']:
            polizasToF = polizasToF.filter(contractor__in = list(dataToFilter['ccpr']))
        if dataToFilter['cgpr']:
            polizasToF = polizasToF.filter(contractor__group__in = list(dataToFilter['cgpr']))
        if dataToFilter['ccepr']:
            polizasToF = polizasToF.filter(contractor__cellule__in = list(dataToFilter['ccepr']))
        if dataToFilter['crpr']:
            polizasToF = polizasToF.filter(contractor__vendor__in = list(dataToFilter['crpr']))
        if dataToFilter['cspr']:
            polizasToF = polizasToF.filter(contractor__sucursal__in = list(dataToFilter['cspr']))
        # Pólizas ****
        if dataToFilter['pppr']:
            polizasToF = polizasToF.filter(pk__in = list(dataToFilter['pppr']))
        if dataToFilter['pgpr']:
            polizasToF = polizasToF.filter(contractor__group__in = list(dataToFilter['pgpr']))
        if dataToFilter['pcepr']:
            polizasToF = polizasToF.filter(celula__in = list(dataToFilter['pcepr']))
        if dataToFilter['prpr']:
            polizasToF = polizasToF.filter(ref_policy__referenciador__in = list(dataToFilter['prpr']))
        if dataToFilter['pspr']:
            polizasToF = polizasToF.filter(sucursal__in = list(dataToFilter['pspr']))
        if dataToFilter['papr']:
            polizasToF = polizasToF.filter(groupinglevel__in = list(dataToFilter['papr']))
        if dataToFilter['pcapr']:
            polizasToF = polizasToF.filter(clave__in = list(dataToFilter['pcapr']))
        if dataToFilter['psrpr']:
            polizasToF = polizasToF.filter(subramo__subramo_code__in = list(dataToFilter['psrpr']))
        if dataToFilter['paspr']:
            polizasToF = polizasToF.filter(aseguradora__in = list(dataToFilter['paspr']))
        if dataToFilter['pstpr']:
            polizasToF = polizasToF.filter(status__in = list(dataToFilter['pstpr']))
        
        polizasCT = polizasCl.filter(document_type = 12, parent__in = list(polizasToF))
        polizasGT = polizasCl.filter(document_type = 6, parent__parent__parent__in = list(polizasToF))
        polizasFin = list(polizasToF.values_list('pk', flat = True)) + list(polizasCT.values_list('pk', flat = True)) + list(polizasGT.values_list('pk', flat = True))
        polizas = polizas.filter(pk__in = list(polizasFin)) 
    # ········ Inicia Excel ········  
    # Empieza insertado de imagen    
    try:
        info_org = getInfoOrg(post)
        if len(info_org['logo']) != 0:
          archivo_imagen = 'https://miurabox.s3.amazonaws.com/cas/' + info_org['logo']
        else:
          archivo_imagen = 'saam.jpg'
    except Exception as d:
        archivo_imagen = 'saam.jpg'
    tipo_reporte = "Reporte de pólizas del " + str(since.strftime("%d/%m/%y")) + " al " + str(until.strftime("%d/%m/%y"))
    row_num = 10 # A partir de aqui pinta la tabla de resultados
    columns = ['Tipo', 'No.Póliza', 'Contratante','Grupo', 'Email', 'Teléfono', 'Proveedor', 'Subramo',
            'Forma de Pago', 'Estatus', 'Inicio Póliza', 'Fin Póliza', 'Moneda', 'Prima Neta', 'Descuento',
            'RPF', 'Derecho', 'IVA', 'Prima Total', 'Comisión','Comisión Porcentaje', 'Clave', 'Sucursal', 'Referenciador', 'Creado por',
            'Fecha creación', 'Observaciones', 'Asegurado', 'Marca', 'Modelo', 'Versión', 'Año',
            'Placas', 'Motor', 'Adaptaciones', 'Conductor','Dirección','Código Postal','Identificador','Ejecutivo Cobranza',
            'Responsable','Ramo','Cancelacion','Renovable','OT/Póliza asociada','Origen Póliza','Paquete', 'Subgrupo','Subsubgrupo',
            'Agrupación','Subagrupación','Subsubagrupación','Clasificación','Línea Negocio','Célula','Fecha Cancelación','Adjuntos','Colectividad/PG completa']
            
    try:
        columns =ast.literal_eval(post['cols1'])
    except:
        pass        
    # ******************************************
    # Empieza insertado de imagen


    columns.insert(3,'Email')
    columns.append('ID_SAAM')    
    try:
        info_org = getInfoOrg(post)
        if len(info_org['logo']) != 0:
            archivo_imagen = 'https://miurabox.s3.amazonaws.com/cas/' + info_org['logo']
        else:
            archivo_imagen = 'saam.jpg'
    except Exception as d:
        archivo_imagen = 'saam.jpg'
    tipo_reporte = "Reporte de pólizas del " + str(since.strftime("%d/%m/%y")) + " al " + str(until.strftime("%d/%m/%y"))
       
    asegurado = []
    antiguedad = []
    referenciador = []
    creadopor = []
    dateRen = []
    otRen = []
    responsable = []
    contratante = []
    contratanteE = []
    contratanteP = []
    contratanteG = []
    collection = []
    clave = []
    cp = []
    caratulas = []
    paquete = []
    valueMA = []  
    valueModA = [] 
    valueVA = [] 
    valueYA = [] 
    valueLPA = [] 
    valueEA = [] 
    valueAdA = [] 
    valueDrA = []
    tipo_admin = []
    nueva = []
    nivelAg = []
    grupo3 = []
    grupo4 = []
    clasifica = []
    poliza_siguiente = []
    poliza_anterior = []
    businessLine = []

    contactos_nombre = []
    contactos_email = []
    contactos_telefono = []
    celulaC = []
    cars_serials = []
    grupo1 = []       
    grupo2 = []
    fechac = []     
    adjuntos = []  
    comisionPercent = []  
    completaPGC = []          
    edocir = []
    descuento = []
    correo_contratante = []
    ids = []
    polizas = polizas.order_by('id')
    bitacora = []
    # polizas = polizas.order_by('natural__full_name','juridical__j_name')
    
    for r in polizas: 
        comment = Comments.objects.filter(
            model = 1, 
            id_model = r.id,
            is_child = False
        )
        if comment.exists():
            comment = comment.last()
            bitacora.append(comment.content)
        else:
            bitacora.append('')

        if r.contractor and r.contractor.email:
            correo_contratante.append(r.contractor.email)
        else:
            correo_contratante.append('')
        ids.append(r.id)
        completa = ''
        comp = r.comision_percent
        adj = 'No'
        if PolizasFile.objects.filter(owner__id = r.id, org_name= r.org_name).exists():
            adj = 'Si'
        else:
            adj  ='No'

        if r.state_circulation:
            valstate = r.state_circulation
            try:
                valstate = getStates(int(r.state_circulation))
            except:
                valstate = r.state_circulation
        else:
            valstate = ''
        anterior = OldPolicies.objects.filter(new_policy = r)
        posterior = OldPolicies.objects.filter(base_policy = r)
        fec = ''
        if r.status ==11:
          try:
            fec = r.date_cancel.strftime("%d/%m/%Y") 
          except:
            fec = ''
        else:
          fec = ''
        if anterior.exists():
          anterior = anterior.last()
          poliza_anterior.append(anterior.base_policy.poliza_number)
        else:
          poliza_anterior.append(0)

        if posterior.exists():
          posterior = posterior.last()
          try:
            poliza_siguiente.append(posterior.new_policy.poliza_number)
          except:
            poliza_siguiente.append(0)
        else:
          poliza_siguiente.append(0)
        if r.document_type ==3 or r.document_type ==11:
            completa = 'Si'
            if r.document_type ==3:
                if (r.poliza_number ==None or r.poliza_number=='') or (r.receipts_by==1 and Recibos.objects.filter(poliza__id = r.id, receipt_type =1).exclude(status = 0).count() ==0):
                    completa = 'No'
                if Polizas.objects.filter(parent__id = r.id, document_type =4).count() ==0:
                    completa = 'No'
                if Polizas.objects.filter(parent__parent__id = r.id, document_type =5).count() ==0:
                    completa = 'No'
                if Polizas.objects.filter(parent__parent__parent__id = r.id, document_type =6).count() ==0:
                    completa = 'No'
                if Package.objects.filter(policy = r.id, type_package =2).count() ==0:
                    completa = 'No'
                if (r.receipts_by==2 and Polizas.objects.filter(parent__id = r.id, document_type =4).exists()):
                    if Recibos.objects.filter(poliza__in = list(Polizas.objects.filter(parent__id = r.id, document_type =4)), receipt_type =1).exclude(status = 0).count() ==0:
                        completa = 'No'
                if (r.receipts_by==3 and Polizas.objects.filter(parent__parent__parent__id = r.id, document_type =6).exists()):
                    if Recibos.objects.filter(poliza__in = list(Polizas.objects.filter(parent__parent__parent__id = r.id, document_type =6)), receipt_type =1).exclude(status = 0).count() ==0:
                        completa = 'No'
            if r.document_type ==11:
                if (r.poliza_number ==None or r.poliza_number==''):
                    completa = 'No'
                if Polizas.objects.filter(parent__id = r.id, document_type =12).count() ==0:
                    completa = 'No'
        if r.document_type == 12:

            try:
                pn = r.parent.poliza_number
            except:
                pn = ''
            caratulas.append(pn)
            if r.date_cancel:
              if r.status ==11:
                try:
                  fec = r.date_cancel.strftime("%d/%m/%Y") 
                except:
                  fec = ''
              else:
                fec = ''
            else:
              if r.status ==11:
                try:
                  fec = r.parent.date_cancel.strftime("%d/%m/%Y") 
                except:
                  fec = ''
              else:
                fec = ''
        else:
            caratulas.append('')
        if r.document_type == 3:
            tipo_admin.append(r.get_administration_type_display()) 
        else:
            tipo_admin.append('')
        nuevaX = '' 
        if r.address:
           pc = r.address.postal_code
        else:
           pc = ''
        antig = get_antiguedad(r.start_of_validity)
        if r.owner:
            val_owner = r.owner.first_name + ' '+ str(r.owner.last_name)
        else:
            val_owner = ''
        if r.paquete:
            pac = r.paquete.package_name
        else:
            pac = ''
        if r.responsable:
            val_resp = r.responsable.first_name + ' '+ str(r.responsable.last_name)
        else:
            val_resp = ''
        if r.collection_executive:
            val_col = r.collection_executive.first_name + ' '+ str(r.collection_executive.last_name)
        else:
            val_col = ''
        if r.clave:
            try:
                cve = r.clave.name + ' '+ str(r.clave.clave)
                cve = ((((((cve).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
            except Exception as ere:
                cve = ''
        else:
            cve = ''
        # if not r.document_type == 11:
        if r.business_line:
            if int(r.business_line) ==1:
                businessLine_ = 'Comercial'
            elif int(r.business_line) ==2:
                businessLine_ = 'Personal'
            elif int(r.business_line) ==0:
                businessLine_ = 'Otro'
            else:
                try:
                    if int(r.business_line) ==0:
                        businessLine_ = 'Otro'
                    else:
                        businessLine_ = ''
                except:
                    businessLine_ = ''
        else:
            businessLine_ = ''
        if r.contractor:
            contratan = ((((((r.contractor.full_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
            contratanE = r.contractor.email
            contratanP = r.contractor.phone_number
            # contratanG = r.contractor.group.group_name
            if r.contractor.classification:
                clasifica_ = r.contractor.classification.classification_name  
            else:
                clasifica_='-----'
            if r.celula:
                cel = r.celula.celula_name  
            else:
                cel='-----'
            contratanG = ((((((r.contractor.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
            try:
                if r.contractor.group.type_group == 1:
                    contratanG = ((((((r.contractor.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    grupo1_ = ''
                    grupo2_ = ''
                elif r.contractor.group.type_group == 2:
                    grupotype1 = Group.objects.get(pk = r.contractor.group.parent.id)
                    contratanG = ((((((grupotype1.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    grupo1_ = ((((((r.contractor.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    grupo2_ = ''
                elif r.contractor.group.type_group == 3:
                    grupotype1 = Group.objects.get(pk = r.contractor.group.parent.parent.id)
                    contratanG = ((((((grupotype1.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    subgrupotype2 = Group.objects.get(pk = r.contractor.group.parent.id)
                    grupo1_ = ((((((subgrupotype2.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    grupo2_ = ((((((r.contractor.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
            except Exception as e:
                contratanG = ''
                grupo1_ = ''
                grupo2_ = ''
            try:
                if r.groupinglevel:
                    if r.groupinglevel.type_grouping == 1:
                        nivelAg_ = ((((((r.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo3_ = ''
                        grupo4_ = ''
                    elif r.groupinglevel.type_grouping == 2:
                        grupotype1 = GroupingLevel.objects.get(pk = r.groupinglevel.parent.id)
                        nivelAg_ = ((((((grupotype1.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo3_ = ((((((r.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo4_ = ''
                    elif r.groupinglevel.type_grouping == 3:
                        grupotype1 = GroupingLevel.objects.get(pk = r.groupinglevel.parent.parent.id)
                        nivelAg_ = ((((((grupotype1.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        subgrupotype2 = GroupingLevel.objects.get(pk = r.groupinglevel.parent.id)
                        grupo3_ = ((((((subgrupotype2.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo4_ = ((((((r.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                else:
                    nivelAg_ = ''
                    grupo3_ = ''
                    grupo4_ = ''  
            except Exception as e:
                nivelAg_ = ''
                grupo3_ = ''
                grupo4_ = ''

            contacto = ContactInfo.objects.filter(contractor=r.contractor)

            if contacto:
                contacto_nombre = contacto[0].name
                contacto_telefono = contacto[0].phone_number
                contacto_email = contacto[0].email
            else:
                contacto_nombre = ''
                contacto_telefono = ''
                contacto_email = ''

        else:
            contratan = ''
            contratanE = ''
            contratanP = ''
            clasifica_='-----'                
            cel='-----'
            contratanG = ''                
            grupo1_ = ''
            grupo2_ = ''
            nivelAg_ = ''
            grupo3_ = ''
            grupo4_ = '' 
            contacto_nombre = ''
            contacto_telefono = ''
            contacto_email = ''

        try:
            old = OldPolicies.objects.filter(base_policy__id = r.id)
            try:
              date_renovacion = old[0].created_at.strftime("%d/%m/%Y")
            except Exception as rr:
                date_renovacion = 'Por renovar'
            try:
                ot_renovacion = old[0].new_policy.poliza_number if old[0].new_policy.poliza_number else old[0].new_policy.internal_number
            except Exception as rr:
                ot_renovacion = 'Por renovar'
        except Exception as dwe:
            date_renovacion = 'Por renovar'
            ot_renovacion = 'Por renovar'
        try:
            refs_policy = ReferenciadoresInvolved.objects.filter(policy = r.id,is_changed=False).values_list('referenciador__first_name','referenciador__last_name')
            if len(refs_policy):
                v = []
                try:
                    for it in refs_policy:
                        v.append(it[0] +' '+ str(it[1])+str('; '))
                    referenc = v
                except Exception as y:
                    referenc = []
            else:
                referenc = []
        except Exception as e:
            referenc = []

        car_serial = ''
        if r.ramo and r.subramo:
            if r.ramo.ramo_code == 1:
                form = Life.objects.filter(policy = r.id)
                if form:
                    try:
                        if form[0].personal:
                            value = form[0].personal.full_name
                        else:
                            value = ''
                    except Exception as e:
                        value = ''
                    valueM = '' 
                    valueMod = '' 
                    valueV = '' 
                    valueY = ''
                    valueLP = '' 
                    valueE = '' 
                    valueAd = '' 
                    valueDr = ''
                else:
                    value = ''
                    valueM = '' 
                    valueMod = '' 
                    valueV = '' 
                    valueY = ''
                    valueLP = '' 
                    valueE = '' 
                    valueAd = '' 
                    valueDr = ''
            elif r.ramo.ramo_code == 2:
                form = AccidentsDiseases.objects.filter(policy = r.id)
                if form:
                    value = form[0].personal.full_name
                    valueM = '' 
                    valueMod = '' 
                    valueV = '' 
                    valueY = ''
                    valueLP = '' 
                    valueE = '' 
                    valueAd = '' 
                    valueDr = ''
                else:
                    value = ''
                    valueM = '' 
                    valueMod = '' 
                    valueV = '' 
                    valueY = ''
                    valueLP = '' 
                    valueE = '' 
                    valueAd = '' 
                    valueDr = ''
            elif r.subramo.subramo_code == 9:
                form = AutomobilesDamages.objects.filter(policy = r.id)
                if form:
                    try:
                        form[0].model = ((((((form[0].model).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        form[0].version = ((((((form[0].version).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    except Exception as ers:
                        form[0].model = form[0].model
                        form[0].version = form[0].version

                    value = str(form[0].brand) + '-' + str(form[0].model )+ '-' + str(form[0].version)
                    valueM = form[0].brand 
                    valueMod = form[0].model 
                    valueV = form[0].version 
                    valueY = form[0].year 
                    valueLP = form[0].license_plates 
                    valueE = form[0].engine 
                    valueAd = form[0].adjustment 
                    valueDr = form[0].driver

                    car_serial = form[0].serial 
                else:
                    value = ''
                    valueM = '' 
                    valueMod = '' 
                    valueV = '' 
                    valueY = ''
                    valueLP = '' 
                    valueE = '' 
                    valueAd = '' 
                    valueDr = ''
            elif r.ramo.ramo_code == 3 and not r.subramo.subramo_code == 9:
                form = Damages.objects.filter(policy = r.id)
                if form:
                    value = form[0].insured_item
                    valueM = '' 
                    valueMod = '' 
                    valueV = '' 
                    valueY = ''
                    valueLP = '' 
                    valueE = '' 
                    valueAd = '' 
                    valueDr = ''
                else:
                    value = ''
                    valueM = '' 
                    valueMod = '' 
                    valueV = '' 
                    valueY = ''
                    valueLP = '' 
                    valueE = '' 
                    valueAd = '' 
                    valueDr = ''
        else:            
            value = ''
            valueM = '' 
            valueMod = '' 
            valueV = '' 
            valueY = ''
            valueLP = '' 
            valueE = '' 
            valueAd = '' 
            valueDr = ''
        if value:
            value = ((((((value).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'') 
        else:
            value = value
        
        nueva.append('')
        if r.document_type ==12:
            try:
                if r.parent.celula:
                    cel = r.parent.celula.celula_name
                else:
                    cel = '-'
                if r.parent.groupinglevel:
                    if r.parent.groupinglevel.type_grouping == 1:
                        nivelAg_ = ((((((r.parent.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo3_ = ''
                        grupo4_ = ''
                    elif r.parent.groupinglevel.type_grouping == 2:
                        grupotype1 = GroupingLevel.objects.get(pk = r.parent.groupinglevel.parent.id)
                        nivelAg_ = ((((((grupotype1.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo3_ = ((((((r.parent.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo4_ = ''
                    elif r.parent.groupinglevel.type_grouping == 3:
                        grupotype1 = GroupingLevel.objects.get(pk = r.parent.groupinglevel.parent.parent.id)
                        nivelAg_ = ((((((grupotype1.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        subgrupotype2 = GroupingLevel.objects.get(pk = r.parent.groupinglevel.parent.id)
                        grupo3_ = ((((((subgrupotype2.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo4_ = ((((((r.parent.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                else:
                    nivelAg_ = ''
                    grupo3_ = ''
                    grupo4_ = ''  
            except Exception as e:
                nivelAg_ = ''
                grupo3_ = ''
                grupo4_ = ''
        else:
            if r.celula:
                cel = r.celula.celula_name
            else:
                cel = '-'
        asegurado.append(value)
        if r.document_type ==3 or r.document_type ==11:
            completaPGC.append(completa)
        else:
            completaPGC.append('')
        dateRen.append(date_renovacion)
        otRen.append(ot_renovacion)
        referenciador.append(referenc)
        antiguedad.append(antig)
        creadopor.append(val_owner)
        contratante.append(contratan)
        contratanteE.append(contratanE)
        contratanteP.append(contratanP)
        contratanteG.append(contratanG)
        responsable.append(val_resp)
        collection.append(val_col)
        paquete.append(pac)
        clave.append(cve)
        cp.append(pc)
        valueMA.append(valueM) 
        valueModA.append(valueMod)  
        valueVA.append(valueV) 
        valueYA.append(valueY) 
        valueLPA.append(valueLP)  
        valueEA.append(valueE)  
        valueAdA.append(valueAd)  
        valueDrA.append(valueDr) 
        # ---
        nivelAg.append(nivelAg_)
        grupo3.append(grupo3_)
        grupo4.append(grupo4_)
        clasifica.append(clasifica_)
        celulaC.append(cel)
        businessLine.append(businessLine_)

        contactos_nombre.append(contacto_nombre)
        contactos_email.append(contacto_email)
        contactos_telefono.append(contacto_telefono)
        cars_serials.append(car_serial)
        grupo1.append(grupo1_)
        grupo2.append(grupo2_)            
        fechac.append(fec)            
        adjuntos.append(adj)            
        comisionPercent.append(comp)
        edocir.append(valstate)           
        # ----   
    df = pd.DataFrame({
        'Tipo'   : [checkDocumentType(x) for x in polizas.values_list('document_type', flat = True)],
        'No.Póliza'   : polizas.values_list('poliza_number', flat = True),
        'Contratante'   : contratante,
        'Email'   : contratanteE,
        'Grupo'   : contratanteG,
        'Proveedor'   : polizas.values_list('aseguradora__alias', flat = True),
        'Ramo'   : polizas.values_list('ramo__ramo_name', flat = True),
        'Subramo'   : polizas.values_list('subramo__subramo_name', flat = True),
        'Identificador'   : [(((((((x).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')) if x else '' for x in polizas.values_list('identifier', flat = True)],
        'Paquete': paquete,
        'Prima Neta': [ float(x.p_neta) if x.p_neta else 0 for x in polizas],
        'Descuento': list(map(float, polizas.values_list('descuento', flat = True))),
        'RPF': [ float(x.rpf) if x.rpf else 0 for x in polizas],
        'Derecho': [ float(x.derecho) if x.derecho else 0 for x in polizas],
        'IVA': [ float(x.iva) if x.iva else 0 for x in polizas],
        'Prima Total': [ float(x.p_total) if x.p_total else 0 for x in polizas],
        'Comisión': [ float(x.comision) if x.comision else 0 for x in polizas],
        'Comisión Porcentaje': comisionPercent,
        'Forma de Pago'   : [checkPayForm(x) for x in polizas.values_list('forma_de_pago', flat = True)],
        'Asegurado'   : asegurado,
        'Estatus'   : [ checkStatusPolicy(x) for x in  polizas.values_list('status', flat = True)],
        'Inicio Póliza'   : polizas.values_list('start_of_validity', flat = True) ,
        'Fin Póliza'   : polizas.values_list('end_of_validity', flat = True),
        'Referenciador'   : [str(x).replace('[','').replace(']','').replace('\'','').replace(';','') for x in referenciador],
        'Moneda' : [ checkCurrency(x) for x in polizas.values_list('f_currency', flat = True)],
        'Fecha creación'   : polizas.values_list('created_at', flat = True) ,
        'Observaciones'   : [(((((((x).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')) if x else '' for x in polizas.values_list('observations', flat = True)],
        'Creado por'   : creadopor,
        'Clave'   : clave,
        'Marca'   : valueMA,
        'Modelo'   : valueModA,
        'Año'   : valueYA,
        'Versión'   : valueVA,
        'Placas'   : valueLPA,
        'Motor'   : valueEA,
        'Adaptaciones'   : valueAdA,
        'Dirección': polizas.values_list('address__administrative_area_level_1', flat = True),
        'Ejecutivo Cobranza'   : collection,
        'Responsable'   : responsable,
        'Sucursal'   : polizas.values_list('sucursal__sucursal_name', flat = True),
        'Renovable'   : [checkRenewable(x) for x in polizas.values_list('is_renewable', flat = True)],
        'OT/Póliza asociada'   : otRen,
        'Origen Póliza'   : nueva,
        'Agrupación'   : nivelAg ,
        'Subagrupación'   : grupo3 ,
        'Subsubagrupación'   : grupo4 ,
        'Clasificación'   : clasifica ,
        'Línea de Negocio'   : businessLine ,
        'Célula'   : celulaC ,
        'Contacto(nombre)'   : contactos_nombre,
        'Contacto(telefono)'   : contactos_telefono,
        'Contacto(email)'   : contactos_email,
        'Carátula': caratulas,
        'Póliza Siguiente': poliza_siguiente,
        'Fecha Cancelación'   : fechac,
        'Adjuntos': adjuntos,
        'Colectividad/PG completa'   : completaPGC,
        'Estado Circulación'   : edocir, 
        'Tipo de Administración': tipo_admin,
        'ID_SAAM'   : polizas.values_list('id', flat = True),
        'Bitacora': bitacora
        # 'Póliza Anterior': poliza_anterior,
        # 'Teléfono'   : contratanteP,
        # 'OT Renovación'   : otRen,
        # 'Código Postal': cp,
        # 'Fecha renovación'   : dateRen,
        # 'Serie'   : cars_serials,
        # 'Conductor'   : valueDrA,
        # 'Motivo de Cancelación'   : polizas.values_list('reason_cancel', flat = True),
        # 'Subgrupo'   : grupo1 ,
        # 'Subsubgrupo'   : grupo2,



        # 'renewed_status': polizas.values_list('renewed_status', flat = True),
        # 'correo_contratante': correo_contratante,
        # 'ids': polizas.values_list('id', flat = True),
        # 'antiguedad'   : antiguedad,

        # 'tipo_reporte': tipo_reporte,
        # 'email_org':info_org['email'],
        # 'phone_org':info_org['phone'],
        # 'webpage_org':info_org['webpage'],
        # 'address_org':info_org['address'],
        # 'urlname_org':info_org['name'],
        } )
    
    try:
        df['Inicio Póliza'] = pd.to_datetime(df['Inicio Póliza'], errors='ignore', format='%D/%B/%Y').dt.date
    except Exception as e:
        df['Inicio Póliza'] = polizas.values_list('start_of_validity', flat = True) 

    try:
        df['Fin Póliza'] = pd.to_datetime(df['Fin Póliza'], errors='ignore', format='%D/%B/%Y').dt.date
    except Exception as e:
        df['Fin Póliza'] = polizas.values_list('end_of_validity', flat = True) 

    try:
        df['Fecha creación'] = pd.to_datetime(df['Fecha creación'], errors='ignore', format='%D/%B/%Y').dt.date
    except Exception as e:
        df['Fecha creación'] = polizas.values_list('created_at', flat = True) 


    
    since = post.get('since').replace('/', '_').replace('-','_')
    until = post.get('until').replace('/', '_').replace('-','_')
    showtime = str(strftime("%Y_%m_%d_%H_%M_%S", gmtime()))
    if self.request.id !=None and self.request.id !='None':
        filename = 'reporte_polizas_%s_%s_%s_%s.xlsx'%(org, self.request.id, str(since).split('T')[0], str(until).split('T')[0])
    else:
        filename = 'reporte_polizas_%s_%s_%s_%s.xlsx'%(org, showtime, str(since).split('T')[0], str(until).split('T')[0])

    writer = pd.ExcelWriter(filename, engine='xlsxwriter')
    # Convert the dataframe to an XlsxWriter Excel object.
    df.to_excel(writer, sheet_name='Sheet1', index=False)
    # Close the Pandas Excel writer and output the Excel file.
    writer.save()
    url_response = 'timeout'
    with open(filename) as f:
        url_response = upload_to_s3(filename, org)
        print(url_response)
        redis_client.publish(self.request.id, url_response)  
        os.remove(filename)        
    data = {}
    data['link'] = url_response
    data['count'] = len(polizas)
    return data
    #return url_response

redis_client = redis.Redis(host='localhost', port=6379, db=0)
@app.task(bind=True, queue=settings.QUEUE)
def fianzas_reportaia_task(self, post):
    org = post['org']    
    report_by = int(post['report_by'])#fechainicio,vigencia
    include_ot = int(post['include_ot'])#1incluir,2soloots,3no incluir
    since = post['since']
    until = post['until']

    fp = [12,24,6,5,4,3,2,1,7,14,15]

    # Filtro de status
    st = [1,2,4,10,11,12,13,14,15]
    
    polizas = Polizas.objects.filter(status__in = st, 
                                     forma_de_pago__in = fp, 
                                     org_name=post['org'],document_type__in = list([7,8]))

    # Filtro de fechas
    try:
        f = "%d/%m/%Y"        
        since = datetime.strptime(since , f)
        until = datetime.strptime(until , f)

    except:
        f = "%m/%d/%Y"        
        since = datetime.strptime(since , f)
        until = datetime.strptime(until , f)

    # Filtro de renovadas
    if int(report_by) == 2:
        date_filters = [Q(start_of_validity__gte=since),Q(start_of_validity__lte = until)]
    elif int(report_by) == 1:
        date_filters = [Q(created_at__gte=since),Q(created_at__lte = until), Q(migrated = False)]
    else:
        date_filters = [Q(start_of_validity__gte=since),Q(start_of_validity__lte = until)]


    if include_ot ==1:
        polizas = polizas
    elif include_ot ==2:
        polizas = polizas.filter(document_type__in = list([7,8]) ,org_name = post['org'], status__in = [1,2])    
    elif include_ot ==3:
        polizas = polizas.filter(document_type__in = list([7,8]) ,org_name = post['org']).exclude(status__in = [1,2])   
    else:
        polizas = polizas 
    try:
        # dataToFilter = {}
        print('post[user]', post['user'],  post['org'])
        dataToFilter = getDataForPerfilRestricted(post['user'], post['org'])
    except Exception as er:
        dataToFilter = {}
    if dataToFilter:
        # Contratantes***
        polizasCl = Polizas.objects.filter(document_type__in = [7,8], org_name = post['org'])
        polizasToF = Polizas.objects.filter(document_type__in = [7,8], org_name = post['org'])
        if dataToFilter['ccpr']:
            polizasToF = polizasToF.filter(contractor__in = list(dataToFilter['ccpr']))
        if dataToFilter['cgpr']:
            polizasToF = polizasToF.filter(contractor__group__in = list(dataToFilter['cgpr']))
        if dataToFilter['ccepr']:
            polizasToF = polizasToF.filter(contractor__cellule__in = list(dataToFilter['ccepr']))
        if dataToFilter['crpr']:
            polizasToF = polizasToF.filter(contractor__vendor__in = list(dataToFilter['crpr']))
        if dataToFilter['cspr']:
            polizasToF = polizasToF.filter(contractor__sucursal__in = list(dataToFilter['cspr']))
        # Pólizas ****
        if dataToFilter['pppr']:
            polizasToF = polizasToF.filter(pk__in = list(dataToFilter['pppr']))
        if dataToFilter['pgpr']:
            polizasToF = polizasToF.filter(contractor__group__in = list(dataToFilter['pgpr']))
        if dataToFilter['pcepr']:
            polizasToF = polizasToF.filter(celula__in = list(dataToFilter['pcepr']))
        if dataToFilter['prpr']:
            polizasToF = polizasToF.filter(ref_policy__referenciador__in = list(dataToFilter['prpr']))
        if dataToFilter['pspr']:
            polizasToF = polizasToF.filter(sucursal__in = list(dataToFilter['pspr']))
        if dataToFilter['papr']:
            polizasToF = polizasToF.filter(groupinglevel__in = list(dataToFilter['papr']))
        if dataToFilter['pcapr']:
            polizasToF = polizasToF.filter(clave__in = list(dataToFilter['pcapr']))
        if dataToFilter['psrpr']:
            polizasToF = polizasToF.filter(subramo__subramo_code__in = list(dataToFilter['psrpr']))
        if dataToFilter['paspr']:
            polizasToF = polizasToF.filter(aseguradora__in = list(dataToFilter['paspr']))
        if dataToFilter['pstpr']:
            polizasToF = polizasToF.filter(status__in = list(dataToFilter['pstpr']))
        
        polizasCT = polizasCl.filter(document_type = 12, parent__in = list(polizasToF))
        polizasGT = polizasCl.filter(document_type = 6, parent__parent__parent__in = list(polizasToF))
        polizasFin = list(polizasToF.values_list('pk', flat = True)) + list(polizasCT.values_list('pk', flat = True)) + list(polizasGT.values_list('pk', flat = True))
        polizas = polizas.filter(pk__in = list(polizasFin)) 
    # ········ Inicia Excel ········  
    # Empieza insertado de imagen    
    try:
        info_org = getInfoOrg(post)
        if len(info_org['logo']) != 0:
            archivo_imagen = 'https://miurabox.s3.amazonaws.com/cas/' + info_org['logo']
        else:
            archivo_imagen = 'saam.jpg'
    except Exception as d:
        archivo_imagen = 'saam.jpg'
    tipo_reporte = "Reporte de fianzas del " + str(since.strftime("%d/%m/%y")) + " al " + str(until.strftime("%d/%m/%y"))
    row_num = 10 # A partir de aqui pinta la tabla de resultados
    columns = ['Tipo', 'No.Póliza', 'Contratante','Grupo', 'Email', 'Teléfono', 'Proveedor', 'Subramo',
            'Forma de Pago', 'Estatus', 'Inicio Póliza', 'Fin Póliza', 'Moneda', 'Prima Neta', 'Descuento',
            'RPF', 'Derecho', 'IVA', 'Prima Total', 'Comisión','Comisión Porcentaje', 'Clave', 'Sucursal', 'Referenciador', 'Creado por',
            'Fecha creación', 'Observaciones', 'Asegurado', 'Marca', 'Modelo', 'Versión', 'Año',
            'Placas', 'Motor', 'Adaptaciones', 'Conductor','Dirección','Código Postal','Identificador','Ejecutivo Cobranza',
            'Responsable','Ramo','Cancelacion','Renovable','OT/Póliza asociada','Origen Póliza','Paquete', 'Subgrupo','Subsubgrupo',
            'Agrupación','Subagrupación','Subsubagrupación','Clasificación','Línea Negocio','Célula','Fecha Cancelación','Adjuntos','Colectividad/PG completa']
            
    try:
        columns =ast.literal_eval(post['cols1'])
    except:
        pass        
    # ******************************************
    # Empieza insertado de imagen


    columns.insert(3,'Email')
    columns.append('ID_SAAM')    
    try:
        info_org = getInfoOrg(post)
        if len(info_org['logo']) != 0:
            archivo_imagen = 'https://miurabox.s3.amazonaws.com/cas/' + info_org['logo']
        else:
            archivo_imagen = 'saam.jpg'
    except Exception as d:
        archivo_imagen = 'saam.jpg'
    tipo_reporte = "Reporte de pólizas del " + str(since.strftime("%d/%m/%y")) + " al " + str(until.strftime("%d/%m/%y"))
       
    asegurado = []
    antiguedad = []
    referenciador = []
    creadopor = []
    dateRen = []
    otRen = []
    responsable = []
    contratante = []
    contratanteE = []
    contratanteP = []
    contratanteG = []
    collection = []
    clave = []
    cp = []
    caratulas = []
    paquete = []
    valueMA = []  
    valueModA = [] 
    valueVA = [] 
    valueYA = [] 
    valueLPA = [] 
    valueEA = [] 
    valueAdA = [] 
    valueDrA = []
    tipo_admin = []
    nueva = []
    nivelAg = []
    grupo3 = []
    grupo4 = []
    clasifica = []
    poliza_siguiente = []
    poliza_anterior = []
    businessLine = []

    contactos_nombre = []
    contactos_email = []
    contactos_telefono = []
    celulaC = []
    cars_serials = []
    grupo1 = []       
    grupo2 = []
    fechac = []     
    adjuntos = []  
    comisionPercent = []  
    completaPGC = []          
    edocir = []
    descuento = []
    correo_contratante = []
    ids = []
    polizas = polizas.order_by('id')
    bitacora = []
    # polizas = polizas.order_by('natural__full_name','juridical__j_name')
    
    for r in polizas: 
        comment = Comments.objects.filter(
            model = 1, 
            id_model = r.id,
            is_child = False
        )
        if comment.exists():
            comment = comment.last()
            bitacora.append(comment.content)
        else:
            bitacora.append('')

        if r.contractor.email:
            correo_contratante.append(r.contractor.email)
        else:
            correo_contratante.append('')
        ids.append(r.id)
        completa = ''
        comp = r.comision_percent
        adj = 'No'
        if PolizasFile.objects.filter(owner__id = r.id, org_name= r.org_name).exists():
            adj = 'Si'
        else:
            adj  ='No'

        if r.state_circulation:
            valstate = r.state_circulation
            try:
                valstate = getStates(int(r.state_circulation))
            except:
                valstate = r.state_circulation
        else:
            valstate = ''
        anterior = OldPolicies.objects.filter(new_policy = r)
        posterior = OldPolicies.objects.filter(base_policy = r)
        fec = ''
        if r.status ==11:
          try:
            fec = r.date_cancel.strftime("%d/%m/%Y") 
          except:
            fec = ''
        else:
          fec = ''
        if anterior.exists():
          anterior = anterior.last()
          poliza_anterior.append(anterior.base_policy.poliza_number)
        else:
          poliza_anterior.append(0)

        if posterior.exists():
          posterior = posterior.last()
          try:
            poliza_siguiente.append(posterior.new_policy.poliza_number)
          except:
            poliza_siguiente.append(0)
        else:
          poliza_siguiente.append(0)
        if r.document_type ==3 or r.document_type ==11:
            completa = 'Si'
            if r.document_type ==3:
                if (r.poliza_number ==None or r.poliza_number=='') or (r.receipts_by==1 and Recibos.objects.filter(poliza__id = r.id, receipt_type =1).exclude(status = 0).count() ==0):
                    completa = 'No'
                if Polizas.objects.filter(parent__id = r.id, document_type =4).count() ==0:
                    completa = 'No'
                if Polizas.objects.filter(parent__parent__id = r.id, document_type =5).count() ==0:
                    completa = 'No'
                if Polizas.objects.filter(parent__parent__parent__id = r.id, document_type =6).count() ==0:
                    completa = 'No'
                if Package.objects.filter(policy = r.id, type_package =2).count() ==0:
                    completa = 'No'
                if (r.receipts_by==2 and Polizas.objects.filter(parent__id = r.id, document_type =4).exists()):
                    if Recibos.objects.filter(poliza__in = list(Polizas.objects.filter(parent__id = r.id, document_type =4)), receipt_type =1).exclude(status = 0).count() ==0:
                        completa = 'No'
                if (r.receipts_by==3 and Polizas.objects.filter(parent__parent__parent__id = r.id, document_type =6).exists()):
                    if Recibos.objects.filter(poliza__in = list(Polizas.objects.filter(parent__parent__parent__id = r.id, document_type =6)), receipt_type =1).exclude(status = 0).count() ==0:
                        completa = 'No'
            if r.document_type ==11:
                if (r.poliza_number ==None or r.poliza_number==''):
                    completa = 'No'
                if Polizas.objects.filter(parent__id = r.id, document_type =12).count() ==0:
                    completa = 'No'
        if r.document_type == 12:

            try:
                pn = r.parent.poliza_number
            except:
                pn = ''
            caratulas.append(pn)
            if r.date_cancel:
              if r.status ==11:
                try:
                  fec = r.date_cancel.strftime("%d/%m/%Y") 
                except:
                  fec = ''
              else:
                fec = ''
            else:
              if r.status ==11:
                try:
                  fec = r.parent.date_cancel.strftime("%d/%m/%Y") 
                except:
                  fec = ''
              else:
                fec = ''
        else:
            caratulas.append('')
        if r.document_type == 3:
            tipo_admin.append(r.get_administration_type_display()) 
        else:
            tipo_admin.append('')
        nuevaX = '' 
        if r.address:
           pc = r.address.postal_code
        else:
           pc = ''
        antig = get_antiguedad(r.start_of_validity)
        if r.owner:
            val_owner = r.owner.first_name + ' '+ str(r.owner.last_name)
        else:
            val_owner = ''
        if r.paquete:
            pac = r.paquete.package_name
        else:
            pac = ''
        if r.responsable:
            val_resp = r.responsable.first_name + ' '+ str(r.responsable.last_name)
        else:
            val_resp = ''
        if r.collection_executive:
            val_col = r.collection_executive.first_name + ' '+ str(r.collection_executive.last_name)
        else:
            val_col = ''
        if r.clave:
            try:
                cve = r.clave.name + ' '+ str(r.clave.clave)
                cve = ((((((cve).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
            except Exception as ere:
                cve = ''
        else:
            cve = ''
        # if not r.document_type == 11:
        if r.business_line:
            if int(r.business_line) ==1:
                businessLine_ = 'Comercial'
            elif int(r.business_line) ==2:
                businessLine_ = 'Personal'
            elif int(r.business_line) ==0:
                businessLine_ = 'Otro'
            else:
                try:
                    if int(r.business_line) ==0:
                        businessLine_ = 'Otro'
                    else:
                        businessLine_ = ''
                except:
                    businessLine_ = ''
        else:
            businessLine_ = ''
        if r.contractor:
            contratan = ((((((r.contractor.full_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
            contratanE = r.contractor.email
            contratanP = r.contractor.phone_number
            # contratanG = r.contractor.group.group_name
            if r.contractor.classification:
                clasifica_ = r.contractor.classification.classification_name  
            else:
                clasifica_='-----'
            if r.celula:
                cel = r.celula.celula_name  
            else:
                cel='-----'
            contratanG = ((((((r.contractor.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
            try:
                if r.contractor.group.type_group == 1:
                    contratanG = ((((((r.contractor.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    grupo1_ = ''
                    grupo2_ = ''
                elif r.contractor.group.type_group == 2:
                    grupotype1 = Group.objects.get(pk = r.contractor.group.parent.id)
                    contratanG = ((((((grupotype1.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    grupo1_ = ((((((r.contractor.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    grupo2_ = ''
                elif r.contractor.group.type_group == 3:
                    grupotype1 = Group.objects.get(pk = r.contractor.group.parent.parent.id)
                    contratanG = ((((((grupotype1.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    subgrupotype2 = Group.objects.get(pk = r.contractor.group.parent.id)
                    grupo1_ = ((((((subgrupotype2.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    grupo2_ = ((((((r.contractor.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
            except Exception as e:
                contratanG = ''
                grupo1_ = ''
                grupo2_ = ''
            try:
                if r.groupinglevel:
                    if r.groupinglevel.type_grouping == 1:
                        nivelAg_ = ((((((r.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo3_ = ''
                        grupo4_ = ''
                    elif r.groupinglevel.type_grouping == 2:
                        grupotype1 = GroupingLevel.objects.get(pk = r.groupinglevel.parent.id)
                        nivelAg_ = ((((((grupotype1.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo3_ = ((((((r.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo4_ = ''
                    elif r.groupinglevel.type_grouping == 3:
                        grupotype1 = GroupingLevel.objects.get(pk = r.groupinglevel.parent.parent.id)
                        nivelAg_ = ((((((grupotype1.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        subgrupotype2 = GroupingLevel.objects.get(pk = r.groupinglevel.parent.id)
                        grupo3_ = ((((((subgrupotype2.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo4_ = ((((((r.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                else:
                    nivelAg_ = ''
                    grupo3_ = ''
                    grupo4_ = ''  
            except Exception as e:
                nivelAg_ = ''
                grupo3_ = ''
                grupo4_ = ''

            contacto = ContactInfo.objects.filter(contractor=r.contractor)

            if contacto:
                contacto_nombre = contacto[0].name
                contacto_telefono = contacto[0].phone_number
                contacto_email = contacto[0].email
            else:
                contacto_nombre = ''
                contacto_telefono = ''
                contacto_email = ''

        else:
            contratan = ''
            contratanE = ''
            contratanP = ''
            clasifica_='-----'                
            cel='-----'
            contratanG = ''                
            grupo1_ = ''
            grupo2_ = ''
            nivelAg_ = ''
            grupo3_ = ''
            grupo4_ = '' 
            contacto_nombre = ''
            contacto_telefono = ''
            contacto_email = ''

        try:
            old = OldPolicies.objects.filter(base_policy__id = r.id)
            try:
              date_renovacion = old[0].created_at.strftime("%d/%m/%Y")
            except Exception as rr:
                date_renovacion = 'Por renovar'
            try:
                ot_renovacion = old[0].new_policy.poliza_number if old[0].new_policy.poliza_number else old[0].new_policy.internal_number
            except Exception as rr:
                ot_renovacion = 'Por renovar'
        except Exception as dwe:
            date_renovacion = 'Por renovar'
            ot_renovacion = 'Por renovar'
        try:
            refs_policy = ReferenciadoresInvolved.objects.filter(policy = r.id,is_chanegd=False).values_list('referenciador__first_name','referenciador__last_name')
            if len(refs_policy):
                v = []
                try:
                    for it in refs_policy:
                        v.append(it[0] +' '+ str(it[1])+str('; '))
                    referenc = v
                except Exception as y:
                    referenc = []
            else:
                referenc = []
        except Exception as e:
            referenc = []

        car_serial = ''
        
        if r.ramo.ramo_code == 1:
            form = Life.objects.filter(policy = r.id)
            if form:
                try:
                    if form[0].personal:
                        value = form[0].personal.full_name
                    else:
                        value = ''
                except Exception as e:
                    value = ''
                valueM = '' 
                valueMod = '' 
                valueV = '' 
                valueY = ''
                valueLP = '' 
                valueE = '' 
                valueAd = '' 
                valueDr = ''
            else:
                value = ''
                valueM = '' 
                valueMod = '' 
                valueV = '' 
                valueY = ''
                valueLP = '' 
                valueE = '' 
                valueAd = '' 
                valueDr = ''
        elif r.ramo.ramo_code == 2:
            form = AccidentsDiseases.objects.filter(policy = r.id)
            if form:
                value = form[0].personal.full_name
                valueM = '' 
                valueMod = '' 
                valueV = '' 
                valueY = ''
                valueLP = '' 
                valueE = '' 
                valueAd = '' 
                valueDr = ''
            else:
                value = ''
                valueM = '' 
                valueMod = '' 
                valueV = '' 
                valueY = ''
                valueLP = '' 
                valueE = '' 
                valueAd = '' 
                valueDr = ''
        elif r.subramo.subramo_code == 9:
            form = AutomobilesDamages.objects.filter(policy = r.id)
            if form:
                try:
                    form[0].model = ((((((form[0].model).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    form[0].version = ((((((form[0].version).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                except Exception as ers:
                    form[0].model = form[0].model
                    form[0].version = form[0].version

                value = str(form[0].brand) + '-' + str(form[0].model )+ '-' + str(form[0].version)
                valueM = form[0].brand 
                valueMod = form[0].model 
                valueV = form[0].version 
                valueY = form[0].year 
                valueLP = form[0].license_plates 
                valueE = form[0].engine 
                valueAd = form[0].adjustment 
                valueDr = form[0].driver

                car_serial = form[0].serial 
            else:
                value = ''
                valueM = '' 
                valueMod = '' 
                valueV = '' 
                valueY = ''
                valueLP = '' 
                valueE = '' 
                valueAd = '' 
                valueDr = ''
        elif r.ramo.ramo_code == 3 and not r.subramo.subramo_code == 9:
            form = Damages.objects.filter(policy = r.id)
            if form:
                value = form[0].insured_item
                valueM = '' 
                valueMod = '' 
                valueV = '' 
                valueY = ''
                valueLP = '' 
                valueE = '' 
                valueAd = '' 
                valueDr = ''
            else:
                value = ''
                valueM = '' 
                valueMod = '' 
                valueV = '' 
                valueY = ''
                valueLP = '' 
                valueE = '' 
                valueAd = '' 
                valueDr = ''
        else:
            value = ''
            valueM = '' 
            valueMod = '' 
            valueV = '' 
            valueY = ''
            valueLP = '' 
            valueE = '' 
            valueAd = '' 
            valueDr = ''
        if value:
            value = ((((((value).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'') 
        else:
            value = value
        
        nueva.append('')
        if r.document_type ==12:
            try:
                if r.parent.celula:
                    cel = r.parent.celula.celula_name
                else:
                    cel = '-'
                if r.parent.groupinglevel:
                    if r.parent.groupinglevel.type_grouping == 1:
                        nivelAg_ = ((((((r.parent.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo3_ = ''
                        grupo4_ = ''
                    elif r.parent.groupinglevel.type_grouping == 2:
                        grupotype1 = GroupingLevel.objects.get(pk = r.parent.groupinglevel.parent.id)
                        nivelAg_ = ((((((grupotype1.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo3_ = ((((((r.parent.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo4_ = ''
                    elif r.parent.groupinglevel.type_grouping == 3:
                        grupotype1 = GroupingLevel.objects.get(pk = r.parent.groupinglevel.parent.parent.id)
                        nivelAg_ = ((((((grupotype1.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        subgrupotype2 = GroupingLevel.objects.get(pk = r.parent.groupinglevel.parent.id)
                        grupo3_ = ((((((subgrupotype2.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo4_ = ((((((r.parent.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                else:
                    nivelAg_ = ''
                    grupo3_ = ''
                    grupo4_ = ''  
            except Exception as e:
                nivelAg_ = ''
                grupo3_ = ''
                grupo4_ = ''
        else:
            if r.celula:
                cel = r.celula.celula_name
            else:
                cel = '-'
        asegurado.append(value)
        if r.document_type ==3 or r.document_type ==11:
            completaPGC.append(completa)
        else:
            completaPGC.append('')
        dateRen.append(date_renovacion)
        otRen.append(ot_renovacion)
        referenciador.append(referenc)
        antiguedad.append(antig)
        creadopor.append(val_owner)
        contratante.append(contratan)
        contratanteE.append(contratanE)
        contratanteP.append(contratanP)
        contratanteG.append(contratanG)
        responsable.append(val_resp)
        collection.append(val_col)
        paquete.append(pac)
        clave.append(cve)
        cp.append(pc)
        valueMA.append(valueM) 
        valueModA.append(valueMod)  
        valueVA.append(valueV) 
        valueYA.append(valueY) 
        valueLPA.append(valueLP)  
        valueEA.append(valueE)  
        valueAdA.append(valueAd)  
        valueDrA.append(valueDr) 
        # ---
        nivelAg.append(nivelAg_)
        grupo3.append(grupo3_)
        grupo4.append(grupo4_)
        clasifica.append(clasifica_)
        celulaC.append(cel)
        businessLine.append(businessLine_)

        contactos_nombre.append(contacto_nombre)
        contactos_email.append(contacto_email)
        contactos_telefono.append(contacto_telefono)
        cars_serials.append(car_serial)
        grupo1.append(grupo1_)
        grupo2.append(grupo2_)            
        fechac.append(fec)            
        adjuntos.append(adj)            
        comisionPercent.append(comp)
        edocir.append(valstate)           
        # ----   
    df = pd.DataFrame({
        'Tipo'   : [checkDocumentType(x) for x in polizas.values_list('document_type', flat = True)],
        'No.Póliza'   : polizas.values_list('poliza_number', flat = True),
        'Contratante'   : contratante,
        'Email'   : contratanteE,
        'Grupo'   : contratanteG,
        'Proveedor'   : polizas.values_list('aseguradora__alias', flat = True),
        'Ramo'   : polizas.values_list('ramo__ramo_name', flat = True),
        'Subramo'   : polizas.values_list('subramo__subramo_name', flat = True),
        'Identificador'   : [(((((((x).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')) if x else '' for x in polizas.values_list('identifier', flat = True)],
        'Paquete': paquete,
        'Prima Neta': [ float(x.p_neta) if x.p_neta else 0 for x in polizas],
        'Descuento': list(map(float, polizas.values_list('descuento', flat = True))),
        'RPF': [ float(x.rpf) if x.rpf else 0 for x in polizas],
        'Derecho': [ float(x.derecho) if x.derecho else 0 for x in polizas],
        'IVA': [ float(x.iva) if x.iva else 0 for x in polizas],
        'Prima Total': [ float(x.p_total) if x.p_total else 0 for x in polizas],
        'Comisión': [ float(x.comision) if x.comision else 0 for x in polizas],
        'Comisión Porcentaje': comisionPercent,
        'Forma de Pago'   : [checkPayForm(x) for x in polizas.values_list('forma_de_pago', flat = True)],
        'Asegurado'   : asegurado,
        'Estatus'   : [ checkStatusPolicy(x) for x in  polizas.values_list('status', flat = True)],
        'Inicio Póliza'   : polizas.values_list('start_of_validity', flat = True) ,
        'Fin Póliza'   : polizas.values_list('end_of_validity', flat = True),
        'Referenciador'   : [str(x).replace('[','').replace(']','').replace('\'','').replace(';','') for x in referenciador],
        'Moneda' : [ checkCurrency(x) for x in polizas.values_list('f_currency', flat = True)],
        'Fecha creación'   : polizas.values_list('created_at', flat = True) ,
        'Observaciones'   : [(((((((x).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')) if x else '' for x in polizas.values_list('observations', flat = True)],
        'Creado por'   : creadopor,
        'Clave'   : clave,
        'Marca'   : valueMA,
        'Modelo'   : valueModA,
        'Año'   : valueYA,
        'Versión'   : valueVA,
        'Placas'   : valueLPA,
        'Motor'   : valueEA,
        'Adaptaciones'   : valueAdA,
        'Dirección': polizas.values_list('address__administrative_area_level_1', flat = True),
        'Ejecutivo Cobranza'   : collection,
        'Responsable'   : responsable,
        'Sucursal'   : polizas.values_list('sucursal__sucursal_name', flat = True),
        'Renovable'   : [checkRenewable(x) for x in polizas.values_list('is_renewable', flat = True)],
        'OT/Póliza asociada'   : otRen,
        'Origen Póliza'   : nueva,
        'Agrupación'   : nivelAg ,
        'Subagrupación'   : grupo3 ,
        'Subsubagrupación'   : grupo4 ,
        'Clasificación'   : clasifica ,
        'Línea de Negocio'   : businessLine ,
        'Célula'   : celulaC ,
        'Contacto(nombre)'   : contactos_nombre,
        'Contacto(telefono)'   : contactos_telefono,
        'Contacto(email)'   : contactos_email,
        'Carátula': caratulas,
        'Póliza Siguiente': poliza_siguiente,
        'Fecha Cancelación'   : fechac,
        'Adjuntos': adjuntos,
        'Colectividad/PG completa'   : completaPGC,
        'Estado Circulación'   : edocir, 
        'Tipo de Administración': tipo_admin,
        'ID_SAAM'   : polizas.values_list('id', flat = True),
        'Bitacora': bitacora
        # 'Póliza Anterior': poliza_anterior,
        # 'Teléfono'   : contratanteP,
        # 'OT Renovación'   : otRen,
        # 'Código Postal': cp,
        # 'Fecha renovación'   : dateRen,
        # 'Serie'   : cars_serials,
        # 'Conductor'   : valueDrA,
        # 'Motivo de Cancelación'   : polizas.values_list('reason_cancel', flat = True),
        # 'Subgrupo'   : grupo1 ,
        # 'Subsubgrupo'   : grupo2,



        # 'renewed_status': polizas.values_list('renewed_status', flat = True),
        # 'correo_contratante': correo_contratante,
        # 'ids': polizas.values_list('id', flat = True),
        # 'antiguedad'   : antiguedad,

        # 'tipo_reporte': tipo_reporte,
        # 'email_org':info_org['email'],
        # 'phone_org':info_org['phone'],
        # 'webpage_org':info_org['webpage'],
        # 'address_org':info_org['address'],
        # 'urlname_org':info_org['name'],
        } )
    
    try:
        df['Inicio Póliza'] = pd.to_datetime(df['Inicio Póliza'], errors='ignore', format='%D/%B/%Y').dt.date
    except Exception as e:
        df['Inicio Póliza'] = polizas.values_list('start_of_validity', flat = True) 

    try:
        df['Fin Póliza'] = pd.to_datetime(df['Fin Póliza'], errors='ignore', format='%D/%B/%Y').dt.date
    except Exception as e:
        df['Fin Póliza'] = polizas.values_list('end_of_validity', flat = True) 

    try:
        df['Fecha creación'] = pd.to_datetime(df['Fecha creación'], errors='ignore', format='%D/%B/%Y').dt.date
    except Exception as e:
        df['Fecha creación'] = polizas.values_list('created_at', flat = True) 


    # obj['columns'] = str(list(columns))
    # obj['imagen'] = archivo_imagen
    # obj['registros'] = len(polizas)

    since = post.get('since').replace('/', '_').replace('-','_')
    until = post.get('until').replace('/', '_').replace('-','_')
    showtime = str(strftime("%Y_%m_%d_%H_%M_%S", gmtime()))
    if self.request.id !=None and self.request.id !='None':
        filename = 'reporte_fianzas_%s_%s_%s_%s.xlsx'%(org, self.request.id, str(since).split('T')[0], str(until).split('T')[0])
    else:
        filename = 'reporte_fianzas_%s_%s_%s_%s.xlsx'%(org, showtime, str(since).split('T')[0], str(until).split('T')[0])
    writer = pd.ExcelWriter(filename, engine='xlsxwriter')
    # Convert the dataframe to an XlsxWriter Excel object.
    df.to_excel(writer, sheet_name='Sheet1', index=False)
    # Close the Pandas Excel writer and output the Excel file.
    writer.save()
    url_response = 'timeout'
    with open(filename) as f:
        url_response = upload_to_s3(filename, org)
        print(url_response)
        redis_client.publish(self.request.id, url_response)  
        os.remove(filename)        
    data = {}
    data['link'] = url_response
    data['count'] = len(polizas)
    return data
    #return url_response

redis_client = redis.Redis(host='localhost', port=6379, db=0)
@app.task(bind=True, queue=settings.QUEUE)
def endosos_reportaia_task(self, post,k):
    org = post['org']    
    status = k.get('status')#estatus
    since = post['since']
    until = post['until']
    status1 = []
    list_1 = list(status[0].split("'"))
    for y in list_1[0]:
        if "{}".format(y).isdigit():
            status1.append(y)
    fp = [12,24,6,5,4,3,2,1,7,14,15]

    # Filtro de status
    if not status:
        status1 = list([1,2,3,4,5])
    endorsment_filters = []
    polizas = Polizas.objects.filter(org_name=post['org'],document_type__in = list([1,3,7,8,11,12])).exclude(status = 0)
    # Filtro de fechas
    try:
        f = "%d/%m/%Y"        
        since = datetime.strptime(since , f)
        until = datetime.strptime(until , f)

    except:
        f = "%m/%d/%Y"        
        since = datetime.strptime(since , f)
        until = datetime.strptime(until , f)

    endorsment_filters.append(Q(init_date__gte=since))
    endosos = Endorsement.objects.filter(reduce(operator.and_,endorsment_filters),status__in = list(status1), org_name = post['org'])
    
    try:
        # dataToFilter = {}
        print('post[user]', post['user'],  post['org'])
        dataToFilter = getDataForPerfilRestricted(post['user'], post['org'])
    except Exception as er:
        dataToFilter = {}
    if dataToFilter:
        # Contratantes***
        polizasCl = Polizas.objects.filter(document_type__in = [1,3,7,8,11,12], org_name = post['org'])
        polizasToF = Polizas.objects.filter(document_type__in = [1,3,7,8,11,12], org_name = post['org'])
        if dataToFilter['ccpr']:
            polizasToF = polizasToF.filter(contractor__in = list(dataToFilter['ccpr']))
        if dataToFilter['cgpr']:
            polizasToF = polizasToF.filter(contractor__group__in = list(dataToFilter['cgpr']))
        if dataToFilter['ccepr']:
            polizasToF = polizasToF.filter(contractor__cellule__in = list(dataToFilter['ccepr']))
        if dataToFilter['crpr']:
            polizasToF = polizasToF.filter(contractor__vendor__in = list(dataToFilter['crpr']))
        if dataToFilter['cspr']:
            polizasToF = polizasToF.filter(contractor__sucursal__in = list(dataToFilter['cspr']))
        # Pólizas ****
        if dataToFilter['pppr']:
            polizasToF = polizasToF.filter(pk__in = list(dataToFilter['pppr']))
        if dataToFilter['pgpr']:
            polizasToF = polizasToF.filter(contractor__group__in = list(dataToFilter['pgpr']))
        if dataToFilter['pcepr']:
            polizasToF = polizasToF.filter(celula__in = list(dataToFilter['pcepr']))
        if dataToFilter['prpr']:
            polizasToF = polizasToF.filter(ref_policy__referenciador__in = list(dataToFilter['prpr']))
        if dataToFilter['pspr']:
            polizasToF = polizasToF.filter(sucursal__in = list(dataToFilter['pspr']))
        if dataToFilter['papr']:
            polizasToF = polizasToF.filter(groupinglevel__in = list(dataToFilter['papr']))
        if dataToFilter['pcapr']:
            polizasToF = polizasToF.filter(clave__in = list(dataToFilter['pcapr']))
        if dataToFilter['psrpr']:
            polizasToF = polizasToF.filter(subramo__subramo_code__in = list(dataToFilter['psrpr']))
        if dataToFilter['paspr']:
            polizasToF = polizasToF.filter(aseguradora__in = list(dataToFilter['paspr']))
        if dataToFilter['pstpr']:
            polizasToF = polizasToF.filter(status__in = list(dataToFilter['pstpr']))
        
        polizasCT = polizasCl.filter(document_type = 12, parent__in = list(polizasToF))
        polizasGT = polizasCl.filter(document_type = 6, parent__parent__parent__in = list(polizasToF))
        polizasFin = list(polizasToF.values_list('pk', flat = True)) + list(polizasCT.values_list('pk', flat = True)) + list(polizasGT.values_list('pk', flat = True))
        polizas = polizas.filter(pk__in = list(polizasFin)) 
    endosos = endosos.filter(policy__in = polizas)
    # ········ Inicia Excel ········  
    # Empieza insertado de imagen
    try:
        info_org = getInfoOrg(post)
        if len(info_org['logo']) != 0:
            archivo_imagen = 'https://miurabox.s3.amazonaws.com/cas/' + info_org['logo']
        else:
            archivo_imagen = 'saam.jpg'
    except Exception as d:
        archivo_imagen = 'saam.jpg'
    tipo_reporte = "Reporte de endosos del " + str(since.strftime("%d/%m/%y")) + " al " + str(until.strftime("%d/%m/%y"))
    row_num = 10 # A partir de aqui pinta la tabla de resultados
    tipo_reporte = 'Reporte Endosos'
    columns = ['Tipo', 'No.Endoso', 'Contratante', 'Email', 'Teléfono', 'Proveedor', 'Subramo', 'Forma de Pago', 'Estatus', 
             'Inicio Póliza', 'Fin Póliza', 'Moneda', 'Prima Neta', 'RPF', 'Derecho', 'IVA', 'Prima Total', 'Comisión',
             'Clave', 'Asegurado', 'Descripción', 'Referenciador', 'Fecha de creación', 'Creado por','Folio Interno',
             'Identificador Póliza/Fianza','Responsable','Ejecutivo Cobranza','Ramo','Tipo de endoso','Sucursal','Grupo',
             'Número de Póliza','Subgrupo','Subsubgrupo','Agrupación','Subagrupación','Subsubagrupación','Clasificación',
             'Línea de Negocio','Célula','Folio_Aseguradora']

    # ORDEN
    try:
        columns = request.data['cols1']
    except:
        pass
    try:
        info_org = getInfoOrg(post)
        if len(info_org['logo']) != 0:
            archivo_imagen = 'https://miurabox.s3.amazonaws.com/cas/' + info_org['logo']
        else:
            archivo_imagen = 'saam.jpg'
    except Exception as d:
        archivo_imagen = 'saam.jpg'
    
    asegurado = []
    antiguedad = []
    referenciador = []
    creadopor = []
    dateRen = []
    otRen = []
    responsable = []
    collection = []
    contratante = []
    contratanteE = []
    contratanteP = []
    contratanteG = []
    clave = []
    cp = []
    subramo = []
    ramo = []
    company = []
    startV = []
    endV = []
    currency = []
    grupo1 = []
    grupo2 = []
    nivelAg = []
    grupo3 = []
    grupo4 = []
    clasifica = []
    businessLine = []
    celulaC =[] 
    fc = 1
    folioInsurance =[]
    responsable =[]
    # paqueteramo__ramo_name        
    # endosos = endosos.order_by('policy__document_type','policy__contractor__full_name','policy__juridical__j_name','fianza__natural__full_name','fianza__juridical__j_name')
    endosos = endosos.order_by('policy__contractor__full_name', 'id')

    for r in endosos:
        value = ''
        val_responsable = ''
        folI=''
        fc = 1
        inc=''
        fn=''
        businessLine_ = ''
        if r.policy:
            if r.policy.address:
               pc = r.policy.address.postal_code
            else:
               pc = ''
        else:
            pc = ''
        antig = get_antiguedad(r.init_date)
        if r.owner:
            val_owner = r.owner.first_name + ' '+ str(r.owner.last_name)
        else:
            val_owner = ''
        if r.policy: 
            if r.policy.business_line:
                if int(r.policy.business_line) ==1:
                    businessLine_ = 'Comercial'
                elif int(r.policy.business_line) ==2:
                    businessLine_ = 'Personal'
                elif int(r.policy.business_line) ==0:
                    businessLine_ = 'Otro'
                else:
                    businessLine_ = ''
            else:
                businessLine_ = ''
            try:               
                inc = r.policy.start_of_validity.strftime("%d/%m/%Y")
                fn =r.policy.end_of_validity.strftime("%d/%m/%Y")
            except Exception as f:
                inc =''
                fn = ''
            if r.policy.responsable:
                val_resp = r.policy.responsable.first_name + ' '+ str(r.policy.responsable.last_name)
            else:
                val_resp = ''
            if r.policy.aseguradora:
                cmpa = r.policy.aseguradora.alias
            else:
                cmpa = ''
            if r.policy.subramo:
                sra = r.policy.subramo.subramo_name
            else:
                sra = ''
            if r.policy.ramo:
                ra = r.policy.ramo.ramo_name
            else:
                ra = ''
            if r.policy.collection_executive:
                cllection = r.policy.collection_executive.first_name +' '+str(r.policy.collection_executive.last_name)
            else:
                cllection = ''
            fc = r.policy.f_currency
            if r.policy.clave:
                try:
                    cve = r.policy.clave.name + ' '+ str(r.policy.clave.clave)
                    cve = ((((((cve).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                except Exception as er:
                    cve =''
            else:
                cve = ''
            if r.policy.contractor:
                contratan = ((((((r.policy.contractor.full_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                contratanE = r.policy.contractor.email
                contratanP = r.policy.contractor.phone_number
                # contratanG = r.policy.contractor.group.group_name
                # contratanG = ((((((r.policy.contractor.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                # --------
                if r.policy.contractor.classification:
                    clasifica_ = r.policy.contractor.classification.classification_name  
                else:
                    clasifica_='-----'
                if r.policy.celula:
                    cel = r.policy.celula.celula_name  
                else:
                    cel='-----'
                try:
                    if r.policy.contractor.group.type_group == 1:
                        contratanG = ((((((r.policy.contractor.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo1_ = ''
                        grupo2_ = ''
                    elif r.policy.contractor.group.type_group == 2:
                        grupotype1 = Group.objects.get(pk = r.policy.contractor.group.parent.id)
                        contratanG = ((((((grupotype1.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo1_ = ((((((r.policy.contractor.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo2_ = ''
                    elif r.policy.contractor.group.type_group == 3:
                        grupotype1 = Group.objects.get(pk = r.policy.contractor.group.parent.parent.id)
                        contratanG = ((((((grupotype1.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        subgrupotype2 = Group.objects.get(pk = r.policy.contractor.group.parent.id)
                        grupo1_ = ((((((subgrupotype2.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo2_ = ((((((r.policy.contractor.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                except Exception as e:
                    contratanG = ''
                    grupo1_ = ''
                    grupo2_ = ''
                try:
                    if r.policy.groupinglevel:
                        if r.policy.groupinglevel.type_grouping == 1:
                            nivelAg_ = ((((((r.policy.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                            grupo3_ = ''
                            grupo4_ = ''
                        elif r.policy.groupinglevel.type_grouping == 2:
                            grupotype1 = GroupingLevel.objects.get(pk = r.policy.groupinglevel.parent.id)
                            nivelAg_ = ((((((grupotype1.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                            grupo3_ = ((((((r.policy.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                            grupo4_ = ''
                        elif r.policy.groupinglevel.type_grouping == 3:
                            grupotype1 = GroupingLevel.objects.get(pk = r.policy.groupinglevel.parent.parent.id)
                            nivelAg_ = ((((((grupotype1.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                            subgrupotype2 = GroupingLevel.objects.get(pk = r.policy.groupinglevel.parent.id)
                            grupo3_ = ((((((subgrupotype2.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                            grupo4_ = ((((((r.policy.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    else:
                        nivelAg_ = ''
                        grupo3_ = ''
                        grupo4_ = ''  
                except Exception as e:
                    nivelAg_ = ''
                    grupo3_ = ''
                    grupo4_ = ''
                    # ------------
            else:
                contratan = ''
                contratanE = ''
                contratanP = ''
                clasifica_='-----'
                cel='-----'
                contratanG = ''
                grupo1_ = ''
                grupo2_ = ''
                nivelAg_ = ''
                grupo3_ = ''
                grupo4_ = ''
                # ------------
            if r.policy.ramo.ramo_code == 1:
                form = Life.objects.filter(policy = r.policy.id)
                try:
                    if form:
                        value = form[0].personal.full_name
                    else:
                        value = ''
                except Exception as rt:
                   value = ''
            elif r.policy.ramo.ramo_code == 2:
                form = AccidentsDiseases.objects.filter(policy = r.policy.id)
                if form:
                    value = form[0].personal.full_name
                else:
                    value = ''
            elif r.policy.subramo.subramo_code == 9:
                form = AutomobilesDamages.objects.filter(policy = r.policy.id)
                if form:
                    try:
                        form[0].model = ((((((form[0].model).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        form[0].version = ((((((form[0].version).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    except Exception as ers:
                        form[0].model = form[0].model
                        form[0].version = form[0].version
                    value = str(form[0].brand) + '-' + str(form[0].model) + '-' + str(form[0].version)+ '-' + str(form[0].serial)
                else:
                    value = ''
            elif r.policy.ramo.ramo_code == 3 and not r.policy.subramo.subramo_code == 9:
                form = Damages.objects.filter(policy = r.policy.id)
                if form:
                    value = form[0].insured_item
                else:
                    value = ''
        else:
            val_resp = ''
            contratan =''
            contratanE =''
            contratanP = ''
            contratanG = ''
        try:
            refs_policy = ReferenciadoresInvolved.objects.filter(policy = r.policy.id,is_changed=False).values_list('referenciador__first_name','referenciador__last_name')
            if len(refs_policy):
                v = []
                try:
                    for it in refs_policy:
                        v.append(it[0] +' '+ str(it[1])+str('; '))
                    referenc = v
                except Exception as y:
                    referenc = []
            else:
                referenc = []
        except Exception as e:
            referenc = []
        if value:
            value = ((((((value).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'') 
        else:
            value = value
        folI = r.insurancefolio
        if folI:
            folI = ((((((folI).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'') 
        else:
            folI = folI


        asegurado.append(value)
        referenciador.append(referenc)
        antiguedad.append(antig)
        creadopor.append(val_owner)
        contratante.append(contratan)
        contratanteE.append(contratanE)
        contratanteP.append(contratanP)
        contratanteG.append(contratanG)
        responsable.append(val_resp)
        clave.append(cve)
        company.append(cmpa)
        subramo.append(sra)
        ramo.append(ra)
        collection.append(cllection)
        startV.append(inc)
        endV.append(fn)
        currency.append(fc)
        # ---
        grupo1.append(grupo1_)
        grupo2.append(grupo2_)
        nivelAg.append(nivelAg_)
        grupo3.append(grupo3_)
        grupo4.append(grupo4_)
        clasifica.append(clasifica_)
        celulaC.append(cel)
        businessLine.append(businessLine_)
        folioInsurance.append(folI)

    df = pd.DataFrame({
        'Tipo'   : [checkDocumentType(x) for x in endosos.values_list('policy__document_type', flat = True)],
        'No.Endoso'   : endosos.values_list('number_endorsement', flat = True),
        'Contratante'   : contratante,
        'Email'   : contratanteE,
        'Teléfono'   : contratanteP,
        'Proveedor'   : endosos.values_list('policy__aseguradora__alias', flat = True),
        'Subramo'   : endosos.values_list('policy__subramo__subramo_name', flat = True),
        'Forma de Pago'   : [checkPayForm(x) for x in endosos.values_list('policy__forma_de_pago', flat = True)],
        'Estatus'   : [ checkStatusEndoso(x) for x in  endosos.values_list('status', flat = True)],
        'Inicio Póliza'   : endosos.values_list('policy__start_of_validity', flat = True) ,
        'Fin Póliza'   : endosos.values_list('policy__end_of_validity', flat = True),
        'Moneda' : [ checkCurrency(x) for x in endosos.values_list('policy__f_currency', flat = True)],
        'Prima Neta': [ float(x.p_neta) if x.p_neta else 0 for x in endosos],
        'RPF': [ float(x.rpf) if x.rpf else 0 for x in endosos],
        'Derecho': [ float(x.derecho) if x.derecho else 0 for x in endosos],
        'IVA': [ float(x.iva) if x.iva else 0 for x in endosos],
        'Prima Total': [ float(x.p_total) if x.p_total else 0 for x in endosos],
        'Comisión': [ float(x.comision) if x.comision else 0 for x in endosos],
        'Clave'   : clave,
        'Asegurado'   : asegurado,
        'Descripción'   : [(((((((x).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')) if x else '' for x in endosos.values_list('observations', flat = True)],
        'Referenciador'   : [str(x).replace('[','').replace(']','').replace('\'','').replace(';','') for x in referenciador],
        'Fecha de creación'   : endosos.values_list('created_at', flat = True) ,
        'Creado por'   : creadopor,
        'Folio Interno'   : endosos.values_list('internal_number', flat = True) ,

        'Identificador Póliza/Fianza'   : [(((((((x).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')) if x else '' for x in endosos.values_list('policy__identifier', flat = True)],
        'Responsable'   : responsable,
        'Ejecutivo Cobranza'   : collection,
        'Ramo'   : endosos.values_list('policy__ramo__ramo_name', flat = True),
        'Tipo de  endoso': endosos.values_list('endorsement_type', flat = True),
        'Sucursal'   : endosos.values_list('policy__sucursal__sucursal_name', flat = True),
        'Grupo'   : contratanteG,
        'Número de Póliza': endosos.values_list('policy__poliza_number', flat = True),
        'Subgrupo'   : grupo1 ,

        'Subsubgrupo'   : grupo2 ,
        'Agrupación'   : nivelAg ,
        'Subagrupación'   : grupo3 ,
        'Subsubagrupación'   : grupo4 ,
        'Clasificación'   : clasifica ,
        'Línea de Negocio'   : businessLine ,
        'Célula'   : celulaC ,
        'Folio_Aseguradora'   : folioInsurance ,
        } )
    
    try:
        df['Inicio Póliza'] = pd.to_datetime(df['Inicio Póliza'], errors='ignore', format='%D/%B/%Y').dt.date
    except Exception as e:
        df['Inicio Póliza'] = endosos.values_list('policy__start_of_validity', flat = True) 

    try:
        df['Fin Póliza'] = pd.to_datetime(df['Fin Póliza'], errors='ignore', format='%D/%B/%Y').dt.date
    except Exception as e:
        df['Fin Póliza'] = endosos.values_list('policy__end_of_validity', flat = True) 

    try:
        df['Fecha de creación'] = pd.to_datetime(df['Fecha de creación'], errors='ignore', format='%D/%B/%Y').dt.date
    except Exception as e:
        df['Fecha de creación'] = polizas.values_list('created_at', flat = True) 

    since = post.get('since').replace('/', '_').replace('-','_')
    until = post.get('until').replace('/', '_').replace('-','_')
    showtime = str(strftime("%Y_%m_%d_%H_%M_%S", gmtime()))
    if self.request.id !=None and self.request.id !='None':
        filename = 'reporte_endosos_%s_%s_%s_%s.xlsx'%(org, self.request.id, str(since).split('T')[0], str(until).split('T')[0])
    else:
        filename = 'reporte_endosos_%s_%s_%s_%s.xlsx'%(org, showtime, str(since).split('T')[0], str(until).split('T')[0])
    writer = pd.ExcelWriter(filename, engine='xlsxwriter')
    # Convert the dataframe to an XlsxWriter Excel object.
    df.to_excel(writer, sheet_name='Sheet1', index=False)
    # Close the Pandas Excel writer and output the Excel file.
    writer.save()
    url_response = 'timeout'
    with open(filename) as f:
        url_response = upload_to_s3(filename, org)
        redis_client.publish(self.request.id, url_response)  
        os.remove(filename)       
    data = {}
    data['link'] = url_response
    data['count'] = len(endosos)
    return data
    #return url_response

redis_client = redis.Redis(host='localhost', port=6379, db=0)
@app.task(bind=True, queue=settings.QUEUE)
def siniestros_reportaia_task(self, post,k):
    org = post['org']    
    report_by = post['report_by']
    since = post['since']
    until = post['until']
    endorsment_filters = []
    # Filtro de fechas
    try:
        f = "%d/%m/%Y"        
        since = datetime.strptime(since , f)
        until = datetime.strptime(until , f)

    except:
        f = "%m/%d/%Y"        
        since = datetime.strptime(since , f)
        until = datetime.strptime(until , f)

    polizas = Polizas.objects.filter(org_name=post['org'],document_type__in = list([1,3,7,8,11,12])).exclude(status = 0)
    if report_by == 2:
        date_filters = [Q(updated_at__gte=since),Q(updated_at__lte = until)]
    else:
        date_filters = [Q(fecha_siniestro__gte=since),Q(fecha_siniestro__lte = until)]

    sinisters = Siniestros.objects.filter(reduce(operator.and_, date_filters)).filter(org_name=org)
    
    try:
        # dataToFilter = {}
        print('post[user]', post['user'],  post['org'])
        dataToFilter = getDataForPerfilRestricted(post['user'], post['org'])
    except Exception as er:
        dataToFilter = {}
    if dataToFilter:
        # Contratantes***
        polizasCl = Polizas.objects.filter(document_type__in = [1,3,7,8,11,12], org_name = post['org'])
        polizasToF = Polizas.objects.filter(document_type__in = [1,3,7,8,11,12], org_name = post['org'])
        if dataToFilter['ccpr']:
            polizasToF = polizasToF.filter(contractor__in = list(dataToFilter['ccpr']))
        if dataToFilter['cgpr']:
            polizasToF = polizasToF.filter(contractor__group__in = list(dataToFilter['cgpr']))
        if dataToFilter['ccepr']:
            polizasToF = polizasToF.filter(contractor__cellule__in = list(dataToFilter['ccepr']))
        if dataToFilter['crpr']:
            polizasToF = polizasToF.filter(contractor__vendor__in = list(dataToFilter['crpr']))
        if dataToFilter['cspr']:
            polizasToF = polizasToF.filter(contractor__sucursal__in = list(dataToFilter['cspr']))
        # Pólizas ****
        if dataToFilter['pppr']:
            polizasToF = polizasToF.filter(pk__in = list(dataToFilter['pppr']))
        if dataToFilter['pgpr']:
            polizasToF = polizasToF.filter(contractor__group__in = list(dataToFilter['pgpr']))
        if dataToFilter['pcepr']:
            polizasToF = polizasToF.filter(celula__in = list(dataToFilter['pcepr']))
        if dataToFilter['prpr']:
            polizasToF = polizasToF.filter(ref_policy__referenciador__in = list(dataToFilter['prpr']))
        if dataToFilter['pspr']:
            polizasToF = polizasToF.filter(sucursal__in = list(dataToFilter['pspr']))
        if dataToFilter['papr']:
            polizasToF = polizasToF.filter(groupinglevel__in = list(dataToFilter['papr']))
        if dataToFilter['pcapr']:
            polizasToF = polizasToF.filter(clave__in = list(dataToFilter['pcapr']))
        if dataToFilter['psrpr']:
            polizasToF = polizasToF.filter(subramo__subramo_code__in = list(dataToFilter['psrpr']))
        if dataToFilter['paspr']:
            polizasToF = polizasToF.filter(aseguradora__in = list(dataToFilter['paspr']))
        if dataToFilter['pstpr']:
            polizasToF = polizasToF.filter(status__in = list(dataToFilter['pstpr']))
        
        polizasCT = polizasCl.filter(document_type = 12, parent__in = list(polizasToF))
        polizasGT = polizasCl.filter(document_type = 6, parent__parent__parent__in = list(polizasToF))
        polizasFin = list(polizasToF.values_list('pk', flat = True)) + list(polizasCT.values_list('pk', flat = True)) + list(polizasGT.values_list('pk', flat = True))
        polizas = polizas.filter(pk__in = list(polizasFin)) 
    sinisters = sinisters.filter(poliza__in = polizas)
    # ········ Inicia Excel ········  
    # Empieza insertado de imagen
    try:
        info_org = getInfoOrg(post)
        if len(info_org['logo']) != 0:
            archivo_imagen = 'https://miurabox.s3.amazonaws.com/cas/' + info_org['logo']
        else:
            archivo_imagen = 'saam.jpg'
    except Exception as d:
        archivo_imagen = 'saam.jpg'
    tipo_reporte = "Reporte de Siniestros del " + str(since.strftime("%d/%m/%y")) + " al " + str(until.strftime("%d/%m/%y"))
    row_num = 10 # A partir de aqui pinta la tabla de resultados
    tipo_reporte = 'Reporte Siniestros'
    columns = ['Afectado', 'No Siniestro', 'Folio', 'Estatus', 'No.Póliza', 'Tipo', 'Contratante', 
                   'Proveedor', 'Subramo', 'Ramo', 'Inicio Póliza', 'Fin Póliza', 'Fecha de creación', 
                   'Creado por', 'Versión','Marca','Modelo', 'Número de Serie', 
                   'Reclamado','Procedente','Fecha de Ingreso','Fecha en que Ocurrio','Fecha Compromiso',
                   'Observaciones','Padecimiento','Tipo de Pago','Folio de la Aseguradora','Inicial / Complemento',
                   'Tipo de trámite','Referenciador','Grupo','Bitácora','Subgrupo','Subsubgrupo','Agrupación',
                   'Subagrupación','Subsubagrupación','Clasificación','Línea de Negocio','Célula']

    # ORDEN
    try:
        columns = request.data['cols1']
    except:
        pass
    try:
        info_org = getInfoOrg(post)
        if len(info_org['logo']) != 0:
            archivo_imagen = 'https://miurabox.s3.amazonaws.com/cas/' + info_org['logo']
        else:
            archivo_imagen = 'saam.jpg'
    except Exception as d:
        archivo_imagen = 'saam.jpg'
    
    asegurado = []
    antiguedad = []
    referenciador = []
    creadopor = []
    dateRen = []
    otRen = []
    responsable = []
    contratante = []
    contratanteE = []
    contratanteP = []
    contratanteG = []
    collection = []
    clave = []
    version = []
    numero_serie = []
    padecimiento = []
    modelo = []
    marca = []
    cp = []
    paquete = []
    procedente = []
    comentario = []
    comentario_fecha = []
    reclamado = []
    pago = []
    tipoSiniestro = []
    valTipo = []  
    ti = ''
    vrs = ''
    mrc = ''
    mdl = ''
    pdc = ''
    total = ''
    recla = ''
    tipopago = ''
    tipo_siniestro = ''
    razon =[]
    currency = []
    fc = 1
    prov = []
    subr = []
    rm = []        
    grupo1 = []
    grupo2 = []
    nivelAg = []
    grupo3 = []
    grupo4 = []
    clasifica = []
    businessLine = []
    celulaC =[]
    # sinisters = sinisters.order_by('poliza__contractor__full_name','poliza__juridical__j_name')
    sinisters = sinisters.order_by('id')
    # start_time = time.time()
    for r in sinisters:
        aseg = ''
        subramo = ''
        ramo = ''
        ti = ''
        fc =1 
        vrs = ''
        mrc = ''
        mdl = ''
        pdc = ''
        total = ''
        recla = ''
        tipopago = ''
        tipo_siniestro = ''
        try:
            comment_ = Comments.objects.filter(org_name=r.org_name, model = 5, id_model = r.id).order_by('-created_at')
            if comment_.exists():
                comment = comment_.last().content
                comment_date = comment_.last().created_at
            else:
                comment = 'Sin comentario'
                comment_date = ''
        except Exception as errorr:
            comment = 'Sin comentario'
            comment_date = ''
        if r.poliza.address:
           pc = r.poliza.address.postal_code
        else:
           pc = ''
        antig = get_antiguedad(r.fecha_siniestro)
        if r.owner:
            val_owner = r.owner.first_name + ' '+ str(r.owner.last_name)
        else:
            val_owner = ''
        if r.poliza.paquete:
            pac = r.poliza.paquete.package_name
        else:
            pac = ''
        if r.poliza.responsable:
            val_resp = r.poliza.responsable.first_name + ' '+ str(r.poliza.responsable.last_name)
        else:
            val_resp = ''
        if r.poliza.collection_executive:
            val_col = r.poliza.collection_executive.first_name + ' '+ str(r.poliza.collection_executive.last_name)
        else:
            val_col = ''
        if r.poliza.clave:
            try:
                cve = r.poliza.clave.name + ' '+ str(r.poliza.clave.clave)
                cve = ((((((cve).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
            except Exception as er:
                cve = ''
        else:
            cve = ''
        if r.poliza.document_type == 6:
            if r.poliza.parent.parent.parent.business_line:
                if int(r.poliza.parent.parent.parent.business_line) ==1:
                    businessLine_ = 'Comercial'
                elif int(r.poliza.parent.parent.parent.business_line) ==2:
                    businessLine_ = 'Personal'
                elif int(r.poliza.parent.parent.parent.business_line) ==0:
                    businessLine_ = 'Otro'
                else:
                    businessLine_ = ''
            else:
                if int(r.poliza.parent.parent.parent.business_line) ==0:
                    businessLine_ = 'Otro'
                else:
                    businessLine_ = ''
            try:
                aseg = r.poliza.parent.parent.parent.aseguradora.alias
                subramo = r.poliza.parent.parent.parent.subramo.subramo_name
                ramo = r.poliza.parent.parent.parent.ramo.ramo_name
            except Exception as e:
                aseg = ''
                subramo = ''
                ramo = ''
            fc =r.poliza.parent.parent.parent.f_currency 
            if r.poliza.parent.parent.parent.contractor:
                contratan = ((((((r.poliza.parent.parent.parent.contractor.full_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                contratanE = r.poliza.parent.parent.parent.contractor.email
                contratanP = r.poliza.parent.parent.parent.contractor.phone_number
                # contratanG = r.poliza.parent.parent.parent.contractor.group.group_name
                contratanG = ((((((r.poliza.parent.parent.parent.contractor.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                if r.poliza.parent.parent.parent.contractor.classification:
                    clasifica_ = r.poliza.parent.parent.parent.contractor.classification.classification_name  
                else:
                    clasifica_='-----'
                if r.poliza.parent.parent.parent.celula:
                    cel = r.poliza.parent.parent.parent.celula.celula_name  
                else:
                    cel='-----'
                try:
                    if r.poliza.parent.parent.parent.contractor.group.type_group == 1:
                        contratanG = ((((((r.poliza.parent.parent.parent.contractor.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo1_ = ''
                        grupo2_ = ''
                    elif r.poliza.parent.parent.parent.contractor.group.type_group == 2:
                        grupotype1 = Group.objects.get(pk = r.poliza.parent.parent.parent.contractor.group.parent.id)
                        contratanG = ((((((grupotype1.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo1_ = ((((((r.poliza.parent.parent.parent.contractor.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo2_ = ''
                    elif r.poliza.parent.parent.parent.contractor.group.type_group == 3:
                        grupotype1 = Group.objects.get(pk = r.poliza.parent.parent.parent.contractor.group.parent.parent.id)
                        contratanG = ((((((grupotype1.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        subgrupotype2 = Group.objects.get(pk = r.poliza.parent.parent.parent.contractor.group.parent.id)
                        grupo1_ = ((((((subgrupotype2.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo2_ = ((((((r.poliza.parent.parent.parent.contractor.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                except Exception as e:
                    contratanG = ''
                    grupo1_ = ''
                    grupo2_ = ''
                try:
                    if r.poliza.parent.parent.parent.groupinglevel:
                        if r.poliza.parent.parent.parent.groupinglevel.type_grouping == 1:
                            nivelAg_ = ((((((r.poliza.parent.parent.parent.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                            grupo3_ = ''
                            grupo4_ = ''
                        elif r.poliza.parent.parent.parent.groupinglevel.type_grouping == 2:
                            grupotype1 = GroupingLevel.objects.get(pk = r.poliza.parent.parent.parent.groupinglevel.parent.id)
                            nivelAg_ = ((((((grupotype1.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                            grupo3_ = ((((((r.poliza.parent.parent.parent.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                            grupo4_ = ''
                        elif r.poliza.parent.parent.parent.groupinglevel.type_grouping == 3:
                            grupotype1 = GroupingLevel.objects.get(pk = r.poliza.parent.parent.parent.groupinglevel.parent.parent.id)
                            nivelAg_ = ((((((grupotype1.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                            subgrupotype2 = GroupingLevel.objects.get(pk = r.poliza.parent.parent.parent.groupinglevel.parent.id)
                            grupo3_ = ((((((subgrupotype2.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                            grupo4_ = ((((((r.poliza.parent.parent.parent.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    else:
                        nivelAg_ = ''
                        grupo3_ = ''
                        grupo4_ = ''  
                except Exception as e:
                    nivelAg_ = ''
                    grupo3_ = ''
                    grupo4_ = ''
                    # ------------
            else:
                contratan = ''
                contratanE = ''
                contratanP = ''
                contratanG = ''
                clasifica_='-----'
                cel='-----'
                contratanG = ''
                grupo1_ = ''
                grupo2_ = ''
                nivelAg_ = ''
                grupo3_ = ''
                grupo4_ = ''                       
        else:
            try:
                aseg = r.poliza.aseguradora.alias
                subramo = r.poliza.subramo.subramo_name
                ramo = r.poliza.ramo.ramo_name
            except Exception as e:
                aseg = ''
                subramo = ''
                ramo = ''
            fc  = r.poliza.f_currency
            if r.poliza.business_line:
                if int(r.poliza.business_line) ==1:
                    businessLine_ = 'Comercial'
                elif int(r.poliza.business_line) ==2:
                    businessLine_ = 'Personal'
                elif int(r.poliza.business_line) ==0:
                    businessLine_ = 'Otro'
                else:
                    businessLine_ = ''
            else:
                if r.poliza.business_line in [0, '0', None]:
                  businessLine_ = 'Otro'
                else:
                  businessLine_ = ''
            if r.poliza.contractor:
                contratan = ((((((r.poliza.contractor.full_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                contratanE = r.poliza.contractor.email
                contratanP = r.poliza.contractor.phone_number
                # contratanG = r.poliza.contractor.group.group_name
                contratanG = ((((((r.poliza.contractor.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                if r.poliza.contractor.classification:
                    clasifica_ = r.poliza.contractor.classification.classification_name  
                else:
                    clasifica_='-----'
                if r.poliza.celula:
                    cel = r.poliza.celula.celula_name  
                else:
                    cel='-----'
                try:
                    if r.poliza.contractor.group.type_group == 1:
                        contratanG = ((((((r.poliza.contractor.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo1_ = ''
                        grupo2_ = ''
                    elif r.poliza.contractor.group.type_group == 2:
                        grupotype1 = Group.objects.get(pk = r.poliza.contractor.group.parent.id)
                        contratanG = ((((((grupotype1.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo1_ = ((((((r.poliza.contractor.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo2_ = ''
                    elif r.poliza.contractor.group.type_group == 3:
                        grupotype1 = Group.objects.get(pk = r.poliza.contractor.group.parent.parent.id)
                        contratanG = ((((((grupotype1.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        subgrupotype2 = Group.objects.get(pk = r.poliza.contractor.group.parent.id)
                        grupo1_ = ((((((subgrupotype2.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo2_ = ((((((r.poliza.contractor.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                except Exception as e:
                    contratanG = ''
                    grupo1_ = ''
                    grupo2_ = ''
                try:
                    if r.poliza.groupinglevel:
                        if r.poliza.groupinglevel.type_grouping == 1:
                            nivelAg_ = ((((((r.poliza.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                            grupo3_ = ''
                            grupo4_ = ''
                        elif r.poliza.groupinglevel.type_grouping == 2:
                            grupotype1 = GroupingLevel.objects.get(pk = r.poliza.groupinglevel.parent.id)
                            nivelAg_ = ((((((grupotype1.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                            grupo3_ = ((((((r.poliza.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                            grupo4_ = ''
                        elif r.poliza.groupinglevel.type_grouping == 3:
                            grupotype1 = GroupingLevel.objects.get(pk = r.poliza.groupinglevel.parent.parent.id)
                            nivelAg_ = ((((((grupotype1.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                            subgrupotype2 = GroupingLevel.objects.get(pk = r.poliza.groupinglevel.parent.id)
                            grupo3_ = ((((((subgrupotype2.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                            grupo4_ = ((((((r.poliza.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    else:
                        nivelAg_ = ''
                        grupo3_ = ''
                        grupo4_ = ''  
                except Exception as e:
                    nivelAg_ = ''
                    grupo3_ = ''
                    grupo4_ = ''
                    # ------------
            else:
                contratan = ''
                contratanE = ''
                contratanP = ''
                contratanG = ''
                clasifica_='-----'
                cel='-----'
                contratanG = ''
                grupo1_ = ''
                grupo2_ = ''
                nivelAg_ = ''
                grupo3_ = ''
                grupo4_ = ''
        try:
            old = OldPolicies.objects.filter(base_policy__id = r.poliza.id)
            try:
              date_renovacion = old[0].created_at.strftime("%d/%m/%Y")
            except Exception as rr:
                date_renovacion = 'Por renovar'
            try:
                ot_renovacion = old[0].new_policy.poliza_number if old[0].new_policy.poliza_number else old[0].new_policy.internal_number
            except Exception as rr:
                ot_renovacion = 'Por renovar'
        except Exception as dwe:
            date_renovacion = 'Por renovar'
            ot_renovacion = 'Por renovar'
        try:
            refs_policy = ReferenciadoresInvolved.objects.filter(policy = r.poliza.id,is_changed=False).values_list('referenciador__first_name','referenciador__last_name')
            if len(refs_policy):
                v = []
                try:
                    for it in refs_policy:
                        v.append(it[0] +' '+ str(it[1])+str('; '))
                    referenc = v
                except Exception as y:
                    referenc = []
            else:
                referenc = []
        except Exception as e:
            referenc = []
        
        ns = ''
        if r.poliza.ramo.ramo_code == 1:
            form = Vida.objects.filter(siniestro = r.id)
            if form:
                value = form[0].nombre_afectado                   
                tipopago = checkRazonVida(form[0].razon_siniestro)
            else:
                form = Life.objects.filter(policy = r.poliza.id)
                try:
                    if form:
                        value = form[0].personal.full_name
                    else:
                        value = ''
                except Exception as rt:
                   value = ''
        elif r.poliza.ramo.ramo_code == 2:
            form = Accidentes.objects.filter(siniestro = r.id)
            if form:
                ti = form[0].tipo_siniestro
                tipopago = checkRazonGMM(form[0].razon_siniestro)
                if form[0].padecimiento:
                    pdc = ((((((form[0].padecimiento.descr).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')   
                else:
                    pdc = ''
                recla = form[0].total_reclamado
                total = form[0].total_procedente
                if form[0].titular:
                    value = form[0].titular.full_name
                elif form[0].dependiente:
                    value = form[0].dependiente.full_name
                elif form[0].affected_full_name:
                    value = form[0].affected_full_name
                elif r.aux_affected:
                    value = r.aux_affected
                else:
                    value = ''
            else:
                form = AccidentsDiseases.objects.filter(policy = r.poliza.id)
                if form:
                    value = form[0].personal.full_name
                else:
                    value = ''
        elif r.poliza.subramo.subramo_code == 9:
            form = AutomobilesDamages.objects.filter(policy = r.poliza.id)
            formA = Autos.objects.filter(siniestro = r.id).first()
            if formA:
                tipopago = checkRazonAutos(formA.tipo_siniestro)
            else:
                tipopago = ''
            if form:
                try:
                    form[0].model = ((((((form[0].model).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    form[0].version = ((((((form[0].version).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                except Exception as ers:
                    form[0].model = form[0].model
                    form[0].version = form[0].version
                value =str(form[0].brand) + '-' + str(form[0].model )+ '-' +str(form[0].version)+ '-' +str(form[0].serial)
                mrc = form[0].brand 
                vrs = form[0].version 
                mdl = form[0].model
                ns = form[0].serial

            elif r.aux_affected:
                value = r.aux_affected
            else:
                value = ''
        elif r.poliza.ramo.ramo_code == 3 and not r.poliza.subramo.subramo_code == 9:
            form = Damages.objects.filter(policy = r.poliza.id)                
            tipopago = ''
            if form:
                value = form[0].insured_item
            else:
                value = ''
        else:
            value = 'Sin asegurado'
            val_resp = 'Sin responsable'
            val_cexe = 'Sin ejecutivo'
        if value:
            value = ((((((value).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'') 
        else:
            value = value
        
        prov.append(aseg)
        subr.append(subramo)
        rm.append(ramo)
        asegurado.append(value)
        currency.append(fc)
        dateRen.append(date_renovacion)
        otRen.append(ot_renovacion)
        referenciador.append(referenc)
        antiguedad.append(antig)
        creadopor.append(val_owner)
        contratante.append(contratan)
        contratanteE.append(contratanE)
        contratanteP.append(contratanP)
        contratanteG.append(contratanG)
        responsable.append(val_resp)
        collection.append(val_col)
        paquete.append(pac)
        clave.append(cve)
        cp.append(pc) 
        valTipo.append(ti) 
        padecimiento.append(pdc) 
        marca.append(mrc) 
        version.append(vrs)
        modelo.append(mdl)
        numero_serie.append(ns) 
        reclamado.append(recla) 
        procedente.append(total) 
        pago.append(tipopago) 
        comentario.append(comment) 
        comentario_fecha.append(comment_date) 
        # ---
        grupo1.append(grupo1_)
        grupo2.append(grupo2_)
        nivelAg.append(nivelAg_)
        grupo3.append(grupo3_)
        grupo4.append(grupo4_)
        clasifica.append(clasifica_)
        celulaC.append(cel)
        businessLine.append(businessLine_)
        tipoSiniestro.append(ti)

    statuses = {
      3:'Completada/Procedente',
      5:'Rechazada/ No Procedente',
      6:'En espera/ Solicitud de Información',
      1: 'Pendiente', 
      2:  'En Trámite', 
      4: 'Cancelada', 
      7: 'Reproceso', 
      8: 'Inconformidad'
    }
    df = pd.DataFrame({
        'Afectado'   :asegurado,
        'No Siniestro'   : sinisters.values_list('numero_siniestro', flat = True),
        'Folio'   : sinisters.values_list('folio_interno', flat = True),
        'Estatus'   : [checkStatusSin(x) for x in sinisters.values_list('status', flat = True)],
        'No.Póliza'   : sinisters.values_list('poliza__poliza_number', flat = True),
        'Tipo'   : tipoSiniestro,
        'Contratante'   :contratante,
        'Proveedor'   :sinisters.values_list('poliza__aseguradora__alias', flat = True),
        'Subramo'   : sinisters.values_list('poliza__subramo__subramo_name', flat = True),
        'Ramo'   : sinisters.values_list('poliza__ramo__ramo_name', flat = True),
        'Inicio Póliza'   : sinisters.values_list('poliza__start_of_validity', flat = True) ,
        'Fin Póliza'   : sinisters.values_list('poliza__end_of_validity', flat = True),
        'Fecha de creación'   : sinisters.values_list('created_at', flat = True) ,
        'Creado por'   : creadopor,
        'Versión'   : version,
        'Marca': marca,
        'Modelo': modelo,
        'Número de Serie':numero_serie,
        'Reclamado': reclamado,
        'Procedente': procedente,
        'Fecha de Ingreso': sinisters.values_list('fecha_ingreso', flat = True),
        'Fecha en que Ocurrio': sinisters.values_list('created_at', flat = True),
        'Fecha Compromiso': sinisters.values_list('promise_date', flat = True),
        'Observaciones'   : [(((((((x).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')) if x else '' for x in sinisters.values_list('observations', flat = True)],
        'Padecimiento': padecimiento,
        'Tipo de Pago': tipopago,
        'Folio de la Aseguradora': sinisters.values_list('folio_compania', flat = True),
        'Referenciador': referenciador,
        'Grupo': contratanteG,
        'Bitacora': comentario,
        'Subgrupo': grupo1,
        'Subsubgrupo': grupo2,
        'Agrupación': nivelAg,
        'Subagrupación': grupo3,
        'Subsubagrupación': grupo4,
        'Clasificación': clasifica,
        'Línea de Negocio': businessLine,
        'Célula': celulaC,
        } )
    
    try:
        df['Inicio Póliza'] = pd.to_datetime(df['Inicio Póliza'], errors='ignore', format='%D/%B/%Y').dt.date
    except Exception as e:
        df['Inicio Póliza'] = endosos.values_list('policy__start_of_validity', flat = True) 

    try:
        df['Fin Póliza'] = pd.to_datetime(df['Fin Póliza'], errors='ignore', format='%D/%B/%Y').dt.date
    except Exception as e:
        df['Fin Póliza'] = endosos.values_list('policy__end_of_validity', flat = True) 

    try:
        df['Fecha de creación'] = pd.to_datetime(df['Fecha de creación'], errors='ignore', format='%D/%B/%Y').dt.date
    except Exception as e:
        df['Fecha de creación'] = polizas.values_list('created_at', flat = True) 
    try:
        df['Fecha de Ingreso'] = pd.to_datetime(df['Fecha de Ingreso'], errors='ignore', format='%D/%B/%Y').dt.date
    except Exception as e:
        df['Fecha de Ingreso'] = sinisters.values_list('fecha_ingreso', flat = True) 
    since = post.get('since').replace('/', '_').replace('-','_')
    until = post.get('until').replace('/', '_').replace('-','_')
    showtime = str(strftime("%Y_%m_%d_%H_%M_%S", gmtime()))
    if self.request.id !=None and self.request.id !='None':
        filename = 'reporte_siniestros_%s_%s_%s_%s.xlsx'%(org, self.request.id, str(since).split('T')[0], str(until).split('T')[0])
    else:
        filename = 'reporte_siniestros_%s_%s_%s_%s.xlsx'%(org, showtime, str(since).split('T')[0], str(until).split('T')[0])
    writer = pd.ExcelWriter(filename, engine='xlsxwriter')
    # Convert the dataframe to an XlsxWriter Excel object.
    df.to_excel(writer, sheet_name='Sheet1', index=False)
    # Close the Pandas Excel writer and output the Excel file.
    writer.save()
    url_response = 'timeout'
    with open(filename) as f:
        url_response = upload_to_s3(filename, org)
        redis_client.publish(self.request.id, url_response)  
        os.remove(filename)       
    data = {}
    data['link'] = url_response
    data['count'] = len(sinisters)
    return data
    #return url_response

redis_client = redis.Redis(host='localhost', port=6379, db=0)
@app.task(bind=True, queue=settings.QUEUE)
def certificados_to_evaluate(self, post,k,y):
    org_name = post['org']    
    owner = post['user']
    try:
        owner = User.objects.get(username=owner)
    except User.DoesNotExist:
        owner = None
    print('--owner--',owner)
    errores = []
    continuarNormal = True
    row = 0
    arrayExist = []
    # Perfil restringido
    certificados = k.get('certificados') 
    polizas = Polizas.objects.filter(document_type__in = [1,3,11,7,8,10,4,12], org_name = org_name).exclude(status = 0)
    perfil_restringido_id = post.get('perfil_restringido_id')
    try:
        dataToFilter = getDataForPerfilRestricted(perfil_restringido_id, org_name)
    except Exception as er:
        dataToFilter = {}
    if dataToFilter:
        # Contratantes***
        polizasToF = Polizas.objects.filter(document_type__in = [1,3,11,7,8,10], org_name = org_name).exclude(status = 0)
        polizasCol = Polizas.objects.filter(document_type__in = [12], org_name = org_name)
        polizasGCer = Polizas.objects.filter(document_type__in = [6], org_name = org_name)
        if dataToFilter['ccpr']:
            polizasToF = polizasToF.filter(contractor__in = list(dataToFilter['ccpr']))
        if dataToFilter['cgpr']:
            polizasToF = polizasToF.filter(contractor__group__in = list(dataToFilter['cgpr']))
        if dataToFilter['ccepr']:
            polizasToF = polizasToF.filter(contractor__cellule__in = list(dataToFilter['ccepr']))
        if dataToFilter['crpr']:
            polizasToF = polizasToF.filter(contractor__vendor__in = list(dataToFilter['crpr']))
        if dataToFilter['cspr']:
            polizasToF = polizasToF.filter(contractor__sucursal__in = list(dataToFilter['cspr']))
        # Pólizas ****
        if dataToFilter['pppr']:
            polizasToF = polizasToF.filter(pk__in = list(dataToFilter['pppr']))
        if dataToFilter['pgpr']:
            polizasToF = polizasToF.filter(contractor__group__in = list(dataToFilter['pgpr']))
        if dataToFilter['pcepr']:
            polizasToF = polizasToF.filter(celula__in = list(dataToFilter['pcepr']))
        if dataToFilter['prpr']:
            polizasToF = polizasToF.filter(ref_policy__referenciador__in = list(dataToFilter['prpr']))
        if dataToFilter['pspr']:
            polizasToF = polizasToF.filter(sucursal__in = list(dataToFilter['pspr']))
        if dataToFilter['papr']:
            polizasToF = polizasToF.filter(groupinglevel__in = list(dataToFilter['papr']))
        if dataToFilter['pcapr']:
            polizasToF = polizasToF.filter(clave__in = list(dataToFilter['pcapr']))
        if dataToFilter['psrpr']:
            polizasToF = polizasToF.filter(subramo__subramo_code__in = list(dataToFilter['psrpr']))
        if dataToFilter['paspr']:
            polizasToF = polizasToF.filter(aseguradora__in = list(dataToFilter['paspr']))
        if dataToFilter['pstpr']:
            polizasToF = polizasToF.filter(status__in = list(dataToFilter['pstpr']))
        
        polizasCT = polizasCol.filter(document_type = 12, parent__in = list(polizasToF))
        polizasGT = polizasGCer.filter(Q(parent__parent__parent__in = list(polizasToF)) | Q(parent__in = list(polizasToF)), document_type__in = [4,6],)
        polizasFin = list(polizasToF.values_list('pk', flat = True)) + list(polizasCT.values_list('pk', flat = True)) + list(polizasGT.values_list('pk', flat = True))       
        polizas = polizas.filter(pk__in = list(polizasFin))
    # Perfil Restringido
    if post.get('validar') == True or post.get('validar') == 'True':   
        continuarNormal = True
        row = 0
        arrayExist = []
        numerosCErt = []
        # error = 'El recibo de la fila %s ya está pagado, cancelado, liquidado, conciliado,cerrado,desactivado,precancelado,o anulado no se aplicará el cambio'%(rowNum)
        # errores.append(error)
        # continue   
        caratulaExists = None
        for certs in certificados:     
            certs = certs.replace("'", '"')
            try:
                certs = json.loads(certs)  
            except:
                certs = eval(certs) 
            for a in certs:
                cont = 0
                for b in certs:
                    if (str(a['certificate_number']) == str(b['certificate_number'])) and (str(a['parent']) == str(b['parent'])):
                        cont += 1
                if cont >1:
                    error ='El certificado: '+str(a['certificate_number'])+', esta repetido en la Categoría de la Póliza Grupo ' +str(a['caratula'])
                    errores.append(error)
                try:    
                    certificateExist = Polizas.objects.get(parent__id = a['parent'],caratula = str(a['caratula']), certificate_number = str(a['certificate_number']), document_type = 6, certificado_inciso_activo = True, org_name = org_name)
                    if certificateExist:
                        error ='El certificado: '+str(a['certificate_number'])+', ya existe dentro de la Categoría de la Póliza Grupo ' +str(a['caratula'])
                        errores.append(error)
                except Exception as erc:
                    pass 
            for dat in certs:
                try:    
                    try:            
                        caratulaExists = Polizas.objects.filter(pk = dat['caratula'], document_type = 3, org_name = org_name)[0]                
                    except Exception as erCar:
                        error = 'La Póliza de Grupo no existe (ID '+str(dat['caratula'])+']'
                        errores.append(error)                        
                except Polizas.DoesNotExist:
                    # return Response(status=status.HTTP_404_NOT_FOUND, data = 'No se encontro la Carátula')
                    error = 'No se encontro la Póliza de Grupo: '+str(dat['caratula'])
                    errores.append(error)
                except Polizas.MultipleObjectsReturned:
                    # return Response(status=status.HTTP_404_NOT_FOUND, data = 'Existe más de un registro')
                    error = 'Existe más de un registro de la Póliza de Grupo: '+str(dat['caratula'])
                    errores.append(error)
                if caratulaExists:
                    if caratulaExists.ramo.ramo_code == 1:
                        if not dat['life_policy'] or len(dat['life_policy']) == 0:
                            # return JsonResponse({'data': 'Debe seleccionar el Layout de Vida'}, status=400)
                            error = 'Debe seleccionar el Layout de Vida'
                            errores.append(error)
                    elif caratulaExists.ramo.ramo_code == 2:
                        if not dat['accidents_policy'] or len(dat['accidents_policy']) == 0:                        
                            error = 'Debe seleccionar el Layout de Gastos Médicos'
                            errores.append(error)
                    elif caratulaExists.ramo.ramo_code == 3 and caratulaExists.subramo.subramo_code == 9:
                        if not dat['automobiles_policy'] or len(dat['automobiles_policy']) == 0:
                            # return JsonResponse({'data': 'Debe seleccionar el Layout de Flotillas'}, status=400)                  
                            error = 'Debe seleccionar el Layout de Flotillas'
                            errores.append(error)
                    elif caratulaExists.ramo.ramo_code == 3 and caratulaExists.subramo.subramo_code != 9:
                        if not dat['damages_policy'] or len(dat['damages_policy']) == 0:
                            # return JsonResponse({'data': 'Debe seleccionar el Layout de Daños'}, status=400)                 
                            error = 'Debe seleccionar el Layout de Daños'
                            errores.append(error)   
                else:   
                    # return JsonResponse({'data': 'La colectividad no se consulto, no existe'}, status=400)    
                    print('s--sssssssss',dat['caratula'])        
                    error = 'No existe la Póliza de Grupo: '+str(dat['caratula'])
                    errores.append(error)             
                if dat['life_policy']:
                    try:
                        if dat['life_policy'][0]['personal'] == None or not dat['life_policy'][0]['personal']:
                            # return JsonResponse({'data': 'Debe agregar un Titular'}, status=400)       
                            error = 'El certificado '+str(dat['certificate_number'])+' de la póliza de grupo '+str(dat['caratula'])+' debe llevar un Titular'
                            errores.append(error)       
                        if dat['life_policy'][0]['personal']:
                            if dat['life_policy'][0]['personal']['first_name'] == None or not dat['life_policy'][0]['personal']['first_name']:
                                # return JsonResponse({'data': 'Debe agregar un nombre al Titular'}, status=400)
                                error = 'El certificado '+str(dat['certificate_number'])+' de la póliza de grupo '+str(dat['caratula'])+' debe llevar nombre de Titular'
                                errores.append(error)    
                            if dat['life_policy'][0]['personal']['email'] == None or not dat['life_policy'][0]['personal']['email']:
                                # return JsonResponse({'data': 'Debe agregar un correo electrónico al Titular'}, status=400)
                                error = 'El certificado '+str(dat['certificate_number'])+' de la póliza de grupo '+str(dat['caratula'])+' debe llevar correo electrónico Titular'
                                errores.append(error)    
                            if dat['life_policy'][0]['personal']['last_name'] == None or not dat['life_policy'][0]['personal']['last_name']:
                                # return JsonResponse({'data': 'Debe agregar apellido al Titular'}, status=400)
                                error = 'El certificado '+str(dat['certificate_number'])+' de la póliza de grupo '+str(dat['caratula'])+' debe llevar apellido Titular'
                                errores.append(error)  
                            if dat['life_policy'][0]['personal']['birthdate'] == None or not dat['life_policy'][0]['personal']['birthdate']:
                                # return JsonResponse({'data': 'Debe agregar un Fecha Nacimiento al Titular'}, status=400)
                                error = 'El certificado '+str(dat['certificate_number'])+' de la póliza de grupo '+str(dat['caratula'])+' debe llevar fecha de nacimiento Titular'
                                errores.append(error)  
                            if dat['life_policy'][0]['personal']['sex'] == None or not dat['life_policy'][0]['personal']['sex']:
                                # return JsonResponse({'data': 'Debe agregar un Género(sexo) al Titular'}, status=400)                          
                                error = 'El certificado '+str(dat['certificate_number'])+' de la póliza de grupo '+str(dat['caratula'])+' debe llevar genero Titular'
                                errores.append(error)  
                    except Exception as errorlife:
                        # return JsonResponse({'data': 'Error en asegurados vida'}, status=400)                 
                        error = 'Error en validación de asegurados'%(dat['caratula'])
                        errores.append(error)  

                if dat['accidents_policy']:
                    if dat['accidents_policy'][0]['personal'] == None or not dat['accidents_policy'][0]['personal']:
                        # return JsonResponse({'data': 'Debe agregar un Titular'}, status=400)   
                        error = 'El certificado '+str(dat['certificate_number'])+' de la póliza de grupo '+str(dat['caratula'])+' debe un Titular'
                        errores.append(error)  
                    if dat['accidents_policy'][0]['personal']:
                        if dat['accidents_policy'][0]['personal']['first_name'] == None or not dat['accidents_policy'][0]['personal']['first_name']:
                            # return JsonResponse({'data': 'Debe agregar un nombre al Titular'}, status=400)
                            error = 'El certificado '+str(dat['certificate_number'])+' de la póliza de grupo '+str(dat['caratula'])+' debe llevar nombre Titular'
                            errores.append(error)  
                        if dat['accidents_policy'][0]['personal']['email'] == None or not dat['accidents_policy'][0]['personal']['email']:
                            # return JsonResponse({'data': 'Debe llevar agregar un correo electrónico al Titular'}, status=400)
                            error = 'El certificado '+str(dat['certificate_number'])+' de la póliza de grupo '+str(dat['caratula'])+' debe llevar correo electrónico Titular'
                            errores.append(error)  
                        if dat['accidents_policy'][0]['personal']['last_name'] == None or not dat['accidents_policy'][0]['personal']['last_name']:
                            # return JsonResponse({'data': 'Debe llevar agregar apellido al Titular'}, status=400)
                            error = 'El certificado '+str(dat['certificate_number'])+' de la póliza de grupo '+str(dat['caratula'])+' debe llevar apellido Titular'
                            errores.append(error)  
                        if dat['accidents_policy'][0]['personal']['birthdate'] == None or not dat['accidents_policy'][0]['personal']['birthdate']:
                            # return JsonResponse({'data': 'Debe llevar agregar un Fecha Nacimiento al Titular'}, status=400)
                            error = 'El certificado '+str(dat['certificate_number'])+' de la póliza de grupo '+str(dat['caratula'])+' debe llevar fecha nacimiento Titular'
                            errores.append(error)  
                        if dat['accidents_policy'][0]['personal']['sex'] == None or not dat['accidents_policy'][0]['personal']['sex']:
                            # return JsonResponse({'data': 'Debe agregar un Género(sexo) al Titular'}, status=400)
                            error = 'El certificado '+str(dat['certificate_number'])+' de la póliza de grupo '+str(dat['caratula'])+' debe llevar género Titular'
                            errores.append(error)  
                    
                if dat['automobiles_policy']:
                    if dat['automobiles_policy'][0]['serial'] == None or not dat['automobiles_policy'][0]['serial']:
                        # return JsonResponse({'data': 'Debe agregar un # de Serie'}, status=400)
                        error = 'El certificado '+str(dat['certificate_number'])+' de la póliza de grupo '+str(dat['caratula'])+' debe llevar un # de Serie (Auto)'
                        errores.append(error)  
                    if dat['automobiles_policy'][0]['email'] == None or not dat['automobiles_policy'][0]['email']:
                        # return JsonResponse({'data': 'Debe agregar un Correo electrónico'}, status=400)
                        error = 'El certificado '+str(dat['certificate_number'])+' de la póliza de grupo '+str(dat['caratula'])+' debe llevar un correo electrónico (Auto)'
                        errores.append(error) 
                    if dat['automobiles_policy'][0]['brand'] == None or not dat['automobiles_policy'][0]['brand']:
                        # return JsonResponse({'data': 'Debe agregar una Marca al Auto'}, status=400)
                        error = 'El certificado '+str(dat['certificate_number'])+' de la póliza de grupo '+str(dat['caratula'])+' debe llevar una marca (Auto)'
                        errores.append(error) 
                    if dat['automobiles_policy'][0]['model'] == None or not dat['automobiles_policy'][0]['model']:
                        # return JsonResponse({'data': 'Debe agregar un Modelo al Auto'}, status=400)
                        error = 'El certificado '+str(dat['certificate_number'])+' de la póliza de grupo '+str(dat['caratula'])+' debe llevar un modelo (Auto)'
                        errores.append(error) 
                    if 'year' in dat['automobiles_policy'][0] and dat['automobiles_policy'][0]['year'] == None or not dat['automobiles_policy'][0]['year']:
                        # return JsonResponse({'data': 'Debe agregar un Año al Auto'}, status=400)
                        error = 'El certificado '+str(dat['certificate_number'])+' de la póliza de grupo '+str(dat['caratula'])+' debe llevar un año (Auto)'
                        errores.append(error) 
                    if dat['automobiles_policy'][0]['version'] == None or not dat['automobiles_policy'][0]['version']:
                        # return JsonResponse({'data': 'Debe agregar una Versión al Auto'}, status=400)
                        error = 'El certificado '+str(dat['certificate_number'])+' de la póliza de grupo '+str(dat['caratula'])+' debe llevar una versión (Auto)'
                        errores.append(error) 
                    if dat['automobiles_policy'][0]['usage'] == None or not dat['automobiles_policy'][0]['usage']:
                        # return JsonResponse({'data': 'Debe agregar un Uso al Auto'}, status=400)
                        error = 'El certificado '+str(dat['certificate_number'])+' de la póliza de grupo '+str(dat['caratula'])+' debe llevar un uso (Auto)'
                        errores.append(error) 
                    else:
                        try:
                            usage = int(dat['automobiles_policy'][0]['usage'])
                            if usage in [1,2,3]:
                                print('==2continuar===',usage)
                            else:
                                # return JsonResponse({'data': 'Debe agregar un Uso al Auto entre (1,2,3)'}, status=400)
                                error = 'El certificado '+str(dat['certificate_number'])+' de la póliza de grupo '+str(dat['caratula'])+' debe llevar un uso (1,2,3) al auto'
                                errores.append(error) 
                        except Exception as eroU:
                            # return JsonResponse({'data': 'Debe agregar un Uso Entero al Auto'}, status=400)
                            error = 'El certificado '+str(dat['certificate_number'])+' de la póliza de grupo '+str(dat['caratula'])+' debe llevar un uso (1,2,3) al auto'
                            errores.append(error) 
                            # return Response(str(eroU), status=400)
                    if dat['automobiles_policy'][0]['procedencia'] == None:
                        dat['automobiles_policy'][0]['procedencia'] = 0
                        # return JsonResponse({'data': 'Debe agregar una Procedencia al Auto'}, status=400)
                        # error = 'El certificado '+str(dat['certificate_number'])+' de la póliza de grupo '+str(dat['caratula'])+' debe llevar una Procedencia al Auto'
                        # errores.append(error) 
                    else:
                        try:
                            procedencia = int(dat['automobiles_policy'][0]['procedencia'])
                            if procedencia in [1,2,3,4,0]:
                                print('==3continuar===',procedencia)
                            else:
                                # return JsonResponse({'data': 'Debe agregar una Procedencia al Auto entre (1,2,3,4,0)'}, status=400)
                                error = 'El certificado '+str(dat['certificate_number'])+' de la póliza de grupo '+str(dat['caratula'])+' debe llevar una Procedencia al Auto entre (1,2,3,4,0)'
                                errores.append(error)
                        except Exception as eroU:
                            # return JsonResponse({'data': 'Debe agregar un Uso Entero al Auto'}, status=400)
                            # return Response(str(eroU), status=400)
                            error = 'El certificado '+str(dat['certificate_number'])+' de la póliza de grupo '+str(dat['caratula'])+' debe llevar una Procedencia al Auto entre (1,2,3,4,0)'
                            errores.append(error)
                    if dat['automobiles_policy'][0]['service'] == None or not dat['automobiles_policy'][0]['service']:
                        # return JsonResponse({'data': 'Debe agregar un Servicio al Auto'}, status=400)
                        error = 'El certificado '+str(dat['certificate_number'])+' de la póliza de grupo '+str(dat['caratula'])+' debe llevar un Servicio al Auto'
                        errores.append(error)
                
                if dat['damages_policy']:
                    if caratulaExists:
                        if caratulaExists.subramo.subramo_name == 'Incendio':
                            if dat['damages_policy'][0]['sucursal'] == None or not dat['damages_policy'][0]['sucursal']:
                                # return JsonResponse({'data': 'Debe agregar una Sucursal'}, status=400)
                                error = 'El certificado '+str(dat['certificate_number'])+' de la póliza de grupo '+str(dat['caratula'])+' debe llevar una Sucursal'
                                errores.append(error)
                            if dat['damages_policy'][0]['giro'] == None or not dat['damages_policy'][0]['giro']:
                                # return JsonResponse({'data': 'Debe agregar una Giro'}, status=400)   
                                error = 'El certificado '+str(dat['certificate_number'])+' de la póliza de grupo '+str(dat['caratula'])+' debe llevar un Giro'
                                errores.append(error)                 
                            if dat['damages_policy'][0]['type_construction'] == None or not dat['damages_policy'][0]['type_construction']:
                                # return JsonResponse({'data': 'Debe agregar una Tipo de construcción'}, status=400)
                                error = 'El certificado '+str(dat['certificate_number'])+' de la póliza de grupo '+str(dat['caratula'])+' debe llevar un Tipo de Construcción'
                                errores.append(error)
                            if dat['damages_policy'][0]['no_levels'] == None or not dat['damages_policy'][0]['no_levels']:
                                # return JsonResponse({'data': 'Debe agregar una # niveles'}, status=400)
                                error = 'El certificado '+str(dat['certificate_number'])+' de la póliza de grupo '+str(dat['caratula'])+' debe llevar un #niveles'
                                errores.append(error)
                            if dat['damages_policy'][0]['fhm_zone'] == None or not dat['damages_policy'][0]['fhm_zone']:
                                # return JsonResponse({'data': 'Debe agregar una Zona FHM'}, status=400)
                                error = 'El certificado '+str(dat['certificate_number'])+' de la póliza de grupo '+str(dat['caratula'])+' debe llevar un Zona FHM'
                                errores.append(error)
                            if dat['damages_policy'][0]['tyev_zone'] == None or not dat['damages_policy'][0]['tyev_zone']:
                                # return JsonResponse({'data': 'Debe agregar una Zona TYEV'}, status=400)
                                error = 'El certificado '+str(dat['certificate_number'])+' de la póliza de grupo '+str(dat['caratula'])+' debe llevar un Zona TYEV'
                                errores.append(error)
                            if dat['damages_policy'][0]['email'] == None or not dat['damages_policy'][0]['email']:
                                # return JsonResponse({'data': 'Debe agregar un Correo electrónico'}, status=400)
                                error = 'El certificado '+str(dat['certificate_number'])+' de la póliza de grupo '+str(dat['caratula'])+' debe llevar un correo electrónico'
                                errores.append(error)
                            if dat['damages_policy'][0]['item_address'] == None or not dat['damages_policy'][0]['item_address']:
                                # return JsonResponse({'data': 'Debe agregar una Dirección'}, status=400)
                                error = 'El certificado '+str(dat['certificate_number'])+' de la póliza de grupo '+str(dat['caratula'])+' debe llevar una Dirección'
                                errores.append(error)

                        if caratulaExists.subramo.subramo_name == 'Diversos':
                            if dat['damages_policy'][0]['usage'] == None or not dat['damages_policy'][0]['usage']:
                                # return JsonResponse({'data': 'Debe agregar una Uso al Equipo'}, status=400)
                                error = 'El certificado '+str(dat['certificate_number'])+' de la póliza de grupo '+str(dat['caratula'])+' debe llevar un Uso (Equipo)'
                                errores.append(error)
                            if dat['damages_policy'][0]['brand'] == None or not dat['damages_policy'][0]['brand']:
                                # return JsonResponse({'data': 'Debe agregar un Marca al Equipo'}, status=400)
                                error = 'El certificado '+str(dat['certificate_number'])+' de la póliza de grupo '+str(dat['caratula'])+' debe llevar una marca (Equipo)'
                                errores.append(error)
                            if dat['damages_policy'][0]['model'] == None or not dat['damages_policy'][0]['model']:
                                # return JsonResponse({'data': 'Debe agregar un Modelo al Equipo'}, status=400)
                                error = 'El certificado '+str(dat['certificate_number'])+' de la póliza de grupo '+str(dat['caratula'])+' debe llevar un modelo (Equipo)'
                                errores.append(error)
                            if dat['damages_policy'][0]['construction_year'] == None or not dat['damages_policy'][0]['construction_year']:
                                # return JsonResponse({'data': 'Debe agregar un Año de Construcción'}, status=400)
                                error = 'El certificado '+str(dat['certificate_number'])+' de la póliza de grupo '+str(dat['caratula'])+' debe llevar un año de Construcción (Equipo)'
                                errores.append(error)
                            if dat['damages_policy'][0]['reconstruction_year'] == None or not dat['damages_policy'][0]['reconstruction_year']:
                                # return JsonResponse({'data': 'Debe agregar un Año de Reconstrucción'}, status=400)
                                error = 'El certificado '+str(dat['certificate_number'])+' de la póliza de grupo '+str(dat['caratula'])+' debe llevar un año de reConstrucción (Equipo)'
                                errores.append(error)
                            if dat['damages_policy'][0]['version'] == None or not dat['damages_policy'][0]['version']:
                                # return JsonResponse({'data': 'Debe agregar una Versión'}, status=400)
                                error = 'El certificado '+str(dat['certificate_number'])+' de la póliza de grupo '+str(dat['caratula'])+' debe llevar una versión (Equipo)'
                                errores.append(error)
                            if dat['damages_policy'][0]['serial'] == None or not dat['damages_policy'][0]['serial']:
                                # return JsonResponse({'data': 'Debe agregar un Número de Serie'}, status=400)
                                error = 'El certificado '+str(dat['certificate_number'])+' de la póliza de grupo '+str(dat['caratula'])+' debe llevar un #serie (Equipo)'
                                errores.append(error)
                            if dat['damages_policy'][0]['purchase_value'] == None or not dat['damages_policy'][0]['purchase_value']:
                                # return JsonResponse({'data': 'Debe agregar un Valor de Compra'}, status=400)
                                error = 'El certificado '+str(dat['certificate_number'])+' de la póliza de grupo '+str(dat['caratula'])+' debe llevar un valor de compra (Equipo)'
                                errores.append(error)
                # Certificate number exist
                try:
                    certExist = Polizas.objects.filter(caratula = str(dat['caratula']), document_type = 6, parent = dat['parent'], certificate_number = str(dat['certificate_number'])).exclude(status = 0)
                    if certExist:
                        # return JsonResponse({'data': 'El Número de certificado --> '+str(dat['certificate_number'])+', ya existe dentro de la Colectividad: ' +str(dat['caratula'])}, status=400)
                        error = 'El Número de certificado --> '+str(dat['certificate_number'])+', ya existe dentro de la Colectividad: ' +str(dat['caratula'])
                        errores.append(error)
                except Exception as ess:
                    pass
                if dat['caratula'] == None or not dat['caratula']:
                    # return JsonResponse({'data': 'Debe agregar un ID de Carátula correspondiente'}, status=400)
                    error = 'Debe agregar un ID de Carátula correspondiente (revise que la columna ID_CARATULA este llena en cada registro)'
                    errores.append(error)
                else:
                    try:    
                        try:
                            caratula = Polizas.objects.filter(pk = dat['caratula'], document_type = 3,status__in = [10,14], org_name = org_name)[0]
                        except Exception as erCar:
                            pass
                    except Polizas.DoesNotExist:
                        # return Response(status=status.HTTP_404_NOT_FOUND, data = 'No se encontro la Carátula')
                        error = 'No se encontro la Póliza de Grupo '+str(dat['caratula'])+' (revise que la columna ID_CARATULA)'
                        errores.append(error)
                    except Polizas.MultipleObjectsReturned:
                        # return Response(status=status.HTTP_404_NOT_FOUND, data = 'Existe más de un registro')
                        error = 'Se encontro más de una Póliza de Grupo  '+str(dat['caratula'])+' (revise que la columna ID_CARATULA)'
                        errores.append(error)
                if dat['certificate_number'] == None or not dat['certificate_number']:
                    # return JsonResponse({'data': 'Debe agregar un No. Certificado'}, status=400)
                    error = 'Debe agregar un No. Certificado, Póliza de Grupo '+str(dat['caratula'])
                    errores.append(error)
                if dat['poliza_number'] == None or not dat['poliza_number']:
                    # return JsonResponse({'data': 'Debe agregar un No. de Póliza'}, status=400)
                    error = 'Debe agregar un No. de Póliza '+str(dat['caratula'])
                    errores.append(error)
                if dat['paquete'] == None or not dat['paquete']:
                    # return JsonResponse({'data': 'Debe agregar un Paquete'}, status=400)
                    error = 'Debe agregar un Paquete '+str(dat['caratula'])+' (revise la columna Paquete de cada registro)'
                    errores.append(error)
                if dat['start_of_validity'] == None or not dat['start_of_validity']:
                    # return JsonResponse({'data': 'Debe agregar Fecha de Inicio de Vigencia del certificado'}, status=400)
                    error = 'Debe agregar Fecha de Inicio de Vigencia del certificado '+str(dat['certificate_number'])+' (revise la columna VIGENCIA_INICIO de cada registro)' 
                    errores.append(error)
                if dat['end_of_validity'] == None or not dat['end_of_validity']:
                    # return JsonResponse({'data': 'Debe agregar Fecha de Fin de Vigencia del certificado'}, status=400)
                    error = 'Debe agregar Fecha de Fin de Vigencia del certificado '+str(dat['certificate_number'])+' (revise la columna VIGENCIA_INICIO de cada registro)'
                    errores.append(error)
            
                try:
                    paquete = Package.objects.get(pk= dat['paquete'], org_name = org_name, policy = dat['caratula'])
                except Package.DoesNotExist:
                    # return JsonResponse({'data': 'No existe Paquete dentro de la Póliza de Grupo seleccionada--> '+str(dat['paquete'] +' Póliza de Grupo: '+str(caratulaExists.poliza_number))}, status=400)
                    error = 'No existe Paquete '+str(dat['paquete'])+' dentro de la Póliza de Grupo seleccionada '+str(dat['caratula'])+' (revise la columna PAQUETE)' 
                    errores.append(error)
                except Exception as erPaq:
                    # return JsonResponse({'data': 'No existe Paquete dentro de la Póliza de Grupo seleccionada--> '+str(dat['paquete'] +' Póliza de Grupo: '+str(caratulaExists.poliza_number))}, status=400)
                    error = 'No existe Paquete '+str(dat['paquete'])+' dentro de la Póliza de Grupo seleccionada '+str(dat['caratula'])+' (revise la columna PAQUETE)' 
                    errores.append(error)
                if dat['parent']:
                    try:
                        parentCert = Polizas.objects.filter(pk = int(dat['parent']), document_type__in=[4,5],status__in = [1,10,14], caratula = dat['caratula'], org_name=org_name)[0]
                    except Exception as ee:
                        # return JsonResponse({'data':'No existe el Subgrupo o Categoría --> '+str(dat['parent'])+', del certificado: '+str(dat['parent'])}, status=400)
                        error = 'No existe el Subgrupo o Categoría --> '+str(dat['parent'])+', del certificado: '+str(dat['parent'])
                        errores.append(error)
                    except Polizas.DoesNotExist:
                        # return JsonResponse({'data':'No se encontro el Subgrupo o Categoría asociada'}, status =400)
                        error = 'No se encontro el Subgrupo o Categoría asociada '+str(dat['parent'])
                        errores.append(error)
                    except Polizas.MultipleObjectsReturned:
                        # return JsonResponse({'data':'Existe más de un registro de Subgrupo o Categoría'}, status = 400)
                        error = 'Existe más de un registro de Subgrupo o Categoría '+str(dat['parent'])
                        errores.append(error)
                try:
                    if dat['p_total']:
                        try:
                            ptotal = float(dat['p_total'])
                            pneta = float(dat['p_neta'])
                            rpf = float(dat['rpf'])
                            derecho = float(dat['derecho'])
                            iva = float(dat['iva'])
                            comision = float(dat['comision'])
                        except Exception as ere:
                            # return JsonResponse({'data': 'Error al validar primas de Certificados: '+str(ere)}, status=400)    
                            error = 'Existe más de un registro de Subgrupo o Categoría '+str(ere)
                            errores.append(error)                    
                except Exception as ere1:
                    # return Response(str(ere1), status=400)   
                    error = 'Existe más de un registro de Subgrupo o Categoría '+str(ere1)
                    errores.append(error)
    
        if len(errores) == 0:
            error = 'Layout correcto, puede proceder a Guardar Certificados'
            errores.append(error)

    if post.get('validar') == False or post.get('validar') == 'False':   
        results = []
        try:
            for certs in certificados:     
                certs = certs.replace("'", '"')
                try:
                    certs = json.loads(certs)  
                except:
                    certs = eval(certs)  
                for dt in certs:
                    certExist = None
                    
                    # Certificate number exist
                    try:
                        certExist = Polizas.objects.filter(org_name=org_name,caratula = str(dt['caratula']), document_type = 6, parent = dt['parent'], certificate_number = str(dt['certificate_number'])).exclude(status = 0)
                        if certExist:
                            error = 'El Número de certificado --> '+str(dt['certificate_number'])+', ya existe dentro de la Póliza de Grupo: ' +str(dt['caratula'])
                            errores.append(error)
                        else:
                            certExist = None
                    except Exception as ess:
                        certExist = None
                        print('eror al consulrar cert')
                        pass
                    print('--certE',certExist)
                    if certExist ==None:
                        caratula =None
                        try:
                            try:
                                caratula = Polizas.objects.get(pk = dt['caratula'], org_name = org_name)
                            except Exception as er: 
                                caratula =None                               
                                error = 'La Póliza de Grupo no existe (ID '+str(dt['caratula'])+']'+' en la ORG '+str(org_name)
                                errores.append(error) 
                                pass
                            if caratula !=None:
                                try:
                                    dt['parent'] = Polizas.objects.get(pk = int(dt['parent']), org_name = org_name)            
                                except Exception as e:                            
                                    error = 'La categoría o subgrupo de la póliza de Grupo no existe (ID '+str(dt['parent'])+']'
                                    errores.append(error) 
                                    pass
                                try:
                                    dt['paquete'] = Package.objects.get(pk = int(dt['paquete']), org_name = org_name)            
                                except Exception as err:                      
                                    error = 'El Paquete de la póliza de Grupo no existe (ID '+str(dt['caratula'])+']'
                                    errores.append(error) 
                                    pass
                                try:
                                    obs_ = dt['observations']
                                except Exception as err:
                                    obs_ = ''
                                if owner ==None:
                                    owner = caratula.owner
                                certificado, certificado_instance = Polizas.objects.get_or_create( 
                                        document_type = 6,
                                        certificate_number = dt['certificate_number'],
                                        rec_antiguedad = dt['rec_antiguedad'] if dt['rec_antiguedad'] else None,
                                        start_of_validity = dt['start_of_validity'],
                                        end_of_validity = dt['end_of_validity'],
                                        folio = dt['folio'] if dt['folio'] else '',
                                        forma_de_pago = caratula.forma_de_pago,
                                        status = 14,
                                        clave = caratula.clave,
                                        contractor = caratula.contractor,
                                        org_name = caratula.org_name,
                                        owner = caratula.owner,
                                        paquete = dt['paquete'],
                                        ramo = caratula.ramo,
                                        subramo = caratula.subramo,
                                        f_currency = caratula.f_currency,
                                        administration_type = caratula.administration_type,
                                        caratula = caratula.id,
                                        parent = dt['parent'],
                                        certificado_inciso_activo = True,
                                        poliza_number = dt['poliza_number'],
                                        observations = obs_,
                                        p_total = Decimal(dt['p_total']),
                                        p_neta = Decimal(dt['p_neta']),
                                        derecho = Decimal(dt['derecho']),
                                        iva = Decimal(dt['iva']),
                                        rpf = Decimal(dt['rpf']),
                                        comision = Decimal(dt['comision']),
                                        scraper = False
                                        )  
                               
                                results.append(certificado.id)
                                try:#Error set Attribute
                                    Emaildata = {}
                                    Emaildata['id'] = certificado.id
                                    Emaildata['model'] = 1
                                    # Emaildata.data = request
                                    email_to_admin_filter(Emaildata)
                                except Exception as errorEmail:
                                    try:
                                        model = Emaildata['model']
                                        id_ = Emaildata['id']
                                    except Exception as erEmailPop: 
                                        pass
                                coberturas = dt.pop('coverageInPolicy_policy')
                                for cobertura in coberturas:
                                    CoverageInPolicy.objects.create(policy = certificado, owner = owner, org_name = poliza.org_name, **cobertura)                
                                if dt['life_policy']:
                                    # if str(dt['life_policy'][0]['personal']['certificate']):
                                    # # for life in dt['life_policy']:
                                    try:
                                        personal = dt['life_policy'][0].pop('personal')
                                        beneficiaries = dt['life_policy'][0].pop('beneficiaries_life')
                                        personal['policy'] = certificado
                                        try:
                                            certificadonumber = personal['certificate']
                                            personal.pop('certificate')
                                        except Exception as er_vend: 
                                            pass
                                        try:
                                            personal.pop('type_sum_assured')
                                        except Exception as tysa: 
                                            pass
                                        try:
                                            personal.pop('caratula')
                                        except Exception as tysa: 
                                            pass
                                        try:
                                            personal_information = Personal_Information.objects.create(owner = owner, org_name=org_name , **personal)
                                        except Exception as er:
                                            print('eer22',er)
                                            pass
                                        if personal_information.first_name:
                                            personal_information.first_name =" ".join(personal_information.first_name.split())
                                        else:
                                            personal_information.first_name =personal_information.first_name
                                        if personal_information.last_name:
                                            personal_information.last_name =" ".join(personal_information.last_name.split())
                                        else:
                                            personal_information.last_name =personal_information.last_name
                                        if personal_information.second_last_name:
                                            personal_information.second_last_name =" ".join(personal_information.second_last_name.split())
                                        else:
                                            personal_information.second_last_name = personal_information.second_last_name
                                        personal_information.full_name = (personal_information.first_name + ' ' + personal_information.last_name + ' ' + (personal_information.second_last_name  if personal_information.second_last_name else '')).strip()   
                                        personal_information.save()
                                        life_instance = Life.objects.create(personal = personal_information, policy = certificado, owner = owner, org_name = caratula.org_name,**dt['life_policy'][0])
                                        try:
                                            if personal_information.email:
                                                to_assign, to_assign_created = Pendients.objects.get_or_create(
                                                    email = personal_information.email,
                                                    poliza= certificado,
                                                    is_owner = True
                                                    )
                                        except Exception as errorAss:
                                            pass
                                        try:
                                            for beneficiarie in beneficiaries:
                                                try:
                                                    beneficiarie['clause'] = beneficiarie['familia']
                                                    beneficiarie.pop('familia')
                                                except Exception as tysa: 
                                                    pass
                                                try:
                                                    beneficiarie.pop('caratula')
                                                except Exception as tysa: 
                                                    pass
                                                try:
                                                    certificadonumber = beneficiarie['certificate']
                                                    beneficiarie.pop('certificate')
                                                except Exception as certN: 
                                                    print('--certificate number----',certN)
                                                beneficiarie_instance = Beneficiaries.objects.create(life = life_instance, owner = certificado.owner, org_name = certificado.org_name, **beneficiarie)
                                                if beneficiarie_instance.first_name:
                                                    beneficiarie_instance.first_name =" ".join(beneficiarie_instance.first_name.split())
                                                else:
                                                    beneficiarie_instance.first_name =beneficiarie_instance.first_name
                                                if beneficiarie_instance.last_name:
                                                    beneficiarie_instance.last_name =" ".join(beneficiarie_instance.last_name.split())
                                                else:
                                                    beneficiarie_instance.last_name =beneficiarie_instance.last_name
                                                if beneficiarie_instance.second_last_name:
                                                    beneficiarie_instance.second_last_name =" ".join(beneficiarie_instance.second_last_name.split())
                                                else:
                                                    beneficiarie_instance.second_last_name = beneficiarie_instance.second_last_name
                                                beneficiarie_instance.full_name = str(beneficiarie_instance.first_name) + ' ' + str(beneficiarie_instance.last_name) + ' ' + str(beneficiarie_instance.second_last_name)
                                                beneficiarie_instance.save()
                                        except Exception as ef:
                                            pass
                                    except Exception as ef:
                                        print('errr',ef)
                                        pass
                                if dt['accidents_policy']:
                                    personal = dt['accidents_policy'][0].pop('personal')
                                    try:
                                        clause1 = personal['clause']
                                        personal.pop('clause')
                                    except Exception as clp: 
                                        pass
                                    try:
                                        personal.pop('caratula')
                                    except Exception as tysa: 
                                        pass
                                    try:
                                        personal_information = Personal_Information.objects.create(owner = owner, org_name =org_name , **personal)
                                    except Exception as er:
                                        pass
                                    relationships = dt['accidents_policy'][0].pop('relationship_accident')
                                    personal_information = Personal_Information.objects.create(policy= certificado, owner = owner, org_name = certificado.org_name, **personal)
                                    if personal_information.first_name:
                                        personal_information.first_name =" ".join(personal_information.first_name.split())
                                    else:
                                        personal_information.first_name =personal_information.first_name
                                    if personal_information.last_name:
                                        personal_information.last_name =" ".join(personal_information.last_name.split())
                                    else:
                                        personal_information.last_name =personal_information.last_name
                                    if personal_information.second_last_name:
                                        personal_information.second_last_name =" ".join(personal_information.second_last_name.split())
                                    else:
                                        personal_information.second_last_name = personal_information.second_last_name
                                    personal_information.full_name = str(personal_information.first_name) + ' ' + str(personal_information.last_name) + ' ' + str(personal_information.second_last_name)
                                    personal_information.save()
                                    # Método de creación de usuar
                                    accident_instance = AccidentsDiseases.objects.create(personal = personal_information, policy = certificado, owner = owner, org_name = org_name,**dt['accidents_policy'][0])
                                    try:
                                        if personal_information.email:
                                            to_assign, to_assign_created = Pendients.objects.get_or_create(
                                                email = personal_information.email,
                                                poliza= certificado,
                                                is_owner = True
                                                )
                                    except Exception as errorAss:
                                        pass
                                    for relationship in relationships:
                                        try:
                                            relationship.pop('familia')
                                        except Exception as fam: 
                                            pass
                                        try:
                                            relationship.pop('clause')
                                        except Exception as fam: 
                                            pass
                                        try:
                                            relationship.pop('caratula')
                                        except Exception as tysa: 
                                            pass
                                        relationship_instance = Relationship.objects.create(accident = accident_instance, owner = owner, org_name = org_name, **relationship)
                                        if relationship_instance.first_name:
                                            relationship_instance.first_name =" ".join(relationship_instance.first_name.split())
                                        else:
                                            relationship_instance.first_name =relationship_instance.first_name
                                        if relationship_instance.last_name:
                                            relationship_instance.last_name =" ".join(relationship_instance.last_name.split())
                                        else:
                                            relationship_instance.last_name =relationship_instance.last_name
                                        if relationship_instance.second_last_name:
                                            relationship_instance.second_last_name =" ".join(relationship_instance.second_last_name.split())
                                        else:
                                            relationship_instance.second_last_name = relationship_instance.second_last_name
                                        relationship_instance.full_name = str(relationship_instance.first_name) + ' ' + str(relationship_instance.last_name) + ' ' + str(relationship_instance.second_last_name)
                                        relationship_instance.save()
                                if dt['damages_policy']:
                                    try:
                                        dt['damages_policy'][0].pop('familia')
                                    except Exception as tysa: 
                                        pass
                                    try:
                                        dt['damages_policy'][0].pop('license_plates')
                                    except Exception as tysa: 
                                        pass          
                                    try:
                                        dt['damages_policy'][0].pop('car_owner')
                                    except Exception as tysa: 
                                        pass
                                    try:
                                        certificadonumber = dt['damages_policy'][0]['certificate']
                                        dt['damages_policy'][0].pop('certificate')
                                    except Exception as certN: 
                                        pass
                                    Damages.objects.create(policy = certificado, owner = owner, org_name = org_name,**dt['damages_policy'][0])
                                if dt['automobiles_policy']:
                                    car_email = dt['automobiles_policy'][0].pop('email')
                                    try:
                                        dt['automobiles_policy'][0].pop('familia')
                                    except Exception as tysa: 
                                        pass
                                    try:
                                        certificadonumber = dt['automobiles_policy'][0]['certificate']
                                        dt['automobiles_policy'][0].pop('certificate')
                                    except Exception as certN: 
                                        print('--certificate number----',certN)
                                    AutomobilesDamages.objects.create( email = car_email, policy = certificado, owner = owner, org_name = caratula.org_name,**dt['automobiles_policy'][0])
                                    try:
                                        if car_email:
                                            to_assign, to_assign_created = Pendients.objects.get_or_create(
                                                email = car_email,
                                                poliza= certificado,
                                                is_owner = True
                                                )
                                    except Exception as errorAss:
                                        pass
                                # LOG certificado
                                dataIdent = ' creo Certificado : '+str(dt['certificate_number'])+' en la Carátula: '+str(caratula.poliza_number)
                                original = {'certificado: ':str(dt['certificate_number']),
                                            'paquete: ':str(dt['paquete'].package_name),
                                            }
                                change= {}                        
                                error = 'Certificado creado '+str(dt['certificate_number'])+', en la Carátula: '+str(caratula.poliza_number)
                                errores.append(error)
                                try:
                                    send_log_complete(request.user, org_name, 'POST', 25, '%s' % str(dataIdent),'%s' % str(original),'%s' % str(change), certificado.id)
                                except Exception as eee:
                                    pass
                        except Exception as e:
                            error = 'Error al guardar certificados '+str(e)
                            print('erorr----',e)
                            errores.append(e)
            if len(errores) == 0:
                error = 'Layout correcto, Certificados creados'
                errores.append(error)
        except Exception as e:
            print('-err',e)
            error = 'Error al cargar certificados '+str(e)
            errores.append(error)
        # certificFin = Polizas.objects.filter(pk__in = results, org_name = org_name)                    
        # certFin = ColectividadSerializer(certificFin, context={'request': request}, many = True)  
        # certificados = certFin.data
        # return JsonResponse(certificados, safe = False) 
    if post.get('validar') == True or post.get('validar') == 'True':
        df = pd.DataFrame({
        'Retroalimentación de validación de certificados': errores,
        } )
        showtime = str(strftime("%Y_%m_%d_%H_%M_%S", gmtime()))
        filename = 'reporte_validacioncertificados_%s_%s_%s.xlsx'%(org_name, self.request.id,showtime)
    else:
        df = pd.DataFrame({
        'Retroalimentación de Creación de certificados': errores,
        } )
        showtime = str(strftime("%Y_%m_%d_%H_%M_%S", gmtime()))
        filename = 'reporte_guardadocertificados_%s_%s_%s.xlsx'%(org_name, self.request.id,showtime)
    writer = pd.ExcelWriter(filename, engine='xlsxwriter')
    # Convert the dataframe to an XlsxWriter Excel object.
    df.to_excel(writer, sheet_name='Sheet1', index=False)
    # Close the Pandas Excel writer and output the Excel file.
    writer.save()
    url_response = 'timeout'
    with open(filename) as f:
        url_response = upload_to_s3(filename, org_name)
        try:
            try:
                user_ownern = post.get('user_req')
                user_owner = User.objects.get(username = user_ownern)
            except Exception as uname:
                user_ownern = 'superuser_'+org_name
                user_owner = User.objects.get(username = user_ownern)
            # crear lanotificación
            payload = {
                "seen": False,
                "id_reference":0,
                "assigned":user_ownern,
                "org_name":org_name,
                "owner":user_ownern,
                "description":url_response,
                "model":26,
                "title":'El reporte (retroalimentación certificados) solicitado ha sido generado'
            }
            r = requests.post(settings.API_URL + 'crear-notificacion-reportes/', (payload), verify=False)
        except Exception as er:
            print('no se creo al notificacion retroalimentación certificados',er)
            pass
        print(url_response)
        redis_client.publish(self.request.id, url_response)  
        os.remove(filename)        
    return url_response

redis_client = redis.Redis(host='localhost', port=6379, db=0)
@app.task(bind=True, queue=settings.QUEUE)
def infocontributory_to_evaluate(self, post,k,y):
    org_name = post['org']    
    owner = post['user']
    try:
        owner = User.objects.get(username=owner)
    except UserInfo.DoesNotExist:
        owner = None
    errores = []
    continuarNormal = True
    row = 0
    arrayExist = []
    # Perfil restringido
    polizasToEdit = k.get('polizas') 
    polizas = Polizas.objects.filter(document_type__in = [1,3,11,7,8,10,4,12], org_name = org_name).exclude(status = 0)
    perfil_restringido_id = post.get('perfil_restringido_id')
    try:
        dataToFilter = getDataForPerfilRestricted(perfil_restringido_id, org_name)
    except Exception as er:
        dataToFilter = {}
    if dataToFilter:
        # Contratantes***
        polizasToF = Polizas.objects.filter(document_type__in = [1,3,11,7,8,10], org_name = org_name).exclude(status = 0)
        polizasCol = Polizas.objects.filter(document_type__in = [12], org_name = org_name)
        polizasGCer = Polizas.objects.filter(document_type__in = [6], org_name = org_name)
        if dataToFilter['ccpr']:
            polizasToF = polizasToF.filter(contractor__in = list(dataToFilter['ccpr']))
        if dataToFilter['cgpr']:
            polizasToF = polizasToF.filter(contractor__group__in = list(dataToFilter['cgpr']))
        if dataToFilter['ccepr']:
            polizasToF = polizasToF.filter(contractor__cellule__in = list(dataToFilter['ccepr']))
        if dataToFilter['crpr']:
            polizasToF = polizasToF.filter(contractor__vendor__in = list(dataToFilter['crpr']))
        if dataToFilter['cspr']:
            polizasToF = polizasToF.filter(contractor__sucursal__in = list(dataToFilter['cspr']))
        # Pólizas ****
        if dataToFilter['pppr']:
            polizasToF = polizasToF.filter(pk__in = list(dataToFilter['pppr']))
        if dataToFilter['pgpr']:
            polizasToF = polizasToF.filter(contractor__group__in = list(dataToFilter['pgpr']))
        if dataToFilter['pcepr']:
            polizasToF = polizasToF.filter(celula__in = list(dataToFilter['pcepr']))
        if dataToFilter['prpr']:
            polizasToF = polizasToF.filter(ref_policy__referenciador__in = list(dataToFilter['prpr']))
        if dataToFilter['pspr']:
            polizasToF = polizasToF.filter(sucursal__in = list(dataToFilter['pspr']))
        if dataToFilter['papr']:
            polizasToF = polizasToF.filter(groupinglevel__in = list(dataToFilter['papr']))
        if dataToFilter['pcapr']:
            polizasToF = polizasToF.filter(clave__in = list(dataToFilter['pcapr']))
        if dataToFilter['psrpr']:
            polizasToF = polizasToF.filter(subramo__subramo_code__in = list(dataToFilter['psrpr']))
        if dataToFilter['paspr']:
            polizasToF = polizasToF.filter(aseguradora__in = list(dataToFilter['paspr']))
        if dataToFilter['pstpr']:
            polizasToF = polizasToF.filter(status__in = list(dataToFilter['pstpr']))
        
        polizasCT = polizasCol.filter(document_type = 12, parent__in = list(polizasToF))
        polizasGT = polizasGCer.filter(Q(parent__parent__parent__in = list(polizasToF)) | Q(parent__in = list(polizasToF)), document_type__in = [4,6],)
        polizasFin = list(polizasToF.values_list('pk', flat = True)) + list(polizasCT.values_list('pk', flat = True)) + list(polizasGT.values_list('pk', flat = True))       
        polizas = polizas.filter(pk__in = list(polizasFin))
    # Perfil Restringido
    if post.get('validar') == True or post.get('validar') == 'True':   
        continuarNormal = True
        row = 0
        arrayExist = []
        numerosCErt = []
        # error = 'El recibo de la fila %s ya está pagado, cancelado, liquidado, conciliado,cerrado,desactivado,precancelado,o anulado no se aplicará el cambio'%(rowNum)
        # errores.append(error)
        # continue   
        caratulaExists = None
        for certs in polizasToEdit:     
            certs = certs.replace("'", '"')
            try:
                certs = json.loads(certs)  
            except:
                certs = eval(certs) 
            for dat in certs:
                if dat['start_of_validity'] and dat['end_of_validity']:
                    try:
                        f = "%d/%m/%Y"        
                        i_vig = datetime.strptime(dat['start_of_validity'] , f)
                        f_vig = datetime.strptime(dat['end_of_validity'] , f)
                    except:
                        f = "%m/%d/%Y"        
                        i_vig = datetime.strptime(dat['start_of_validity'] , f)
                        f_vig = datetime.strptime(dat['end_of_validity'] , f)
                # date_filters = [Q(start_of_validity__gte=i_vig),Q(start_of_validity__lte = f_vig)]    
                if (dat['id'] == None or not dat['id']) and (dat['poliza_number'] == '' or dat['start_of_validity'] == '' or dat['end_of_validity'] == ''):
                    error = 'Debe agregar un ID de Carátula correspondiente (revise que la columna ID este llena en cada registro)'
                    errores.append(error)
                elif (dat['id'] and (dat['id'] != 0 or dat['id'] != '0')):
                    try:    
                        try:            
                            caratulaExists = Polizas.objects.get(pk = dat['id'], document_type = 3, org_name = org_name)           
                        except Exception as erCar:
                            error = 'La Póliza de Grupo no existe (ID '+str(dat['id'])+')'
                            errores.append(error)                        
                    except Polizas.DoesNotExist:
                        error = 'No se encontro la Póliza de Grupo: '+str(dat['caratula'])
                        errores.append(error)
                    except Polizas.MultipleObjectsReturned:
                        # return Response(status=status.HTTP_404_NOT_FOUND, data = 'Existe más de un registro')
                        error = 'Existe más de un registro de la Póliza de Grupo: '+str(dat['caratula'])
                        errores.append(error)
                    if caratulaExists:
                        if caratulaExists.ramo.ramo_code == 2:                        
                            error = 'La Póliza de Grupo es de Accidentes y Enfermedades '+str(dat['id'])+', se editará correctamente'
                            errores.append(error)
                        else:                      
                            error = 'La Póliza de Grupo NO es de Accidentes y Enfermedades '+str(dat['id'])+', NO APLICA EL CAMBIO'
                            errores.append(error)                    
                    else:     
                        error = 'No existe la Póliza de Grupo: '+str(dat['id'])
                        errores.append(error) 
                else:
                    if (dat['poliza_number'] != '' and dat['start_of_validity'] != '' and dat['end_of_validity'] != ''):
                        try:    
                            try:            
                                caratulaExists = Polizas.objects.get(poliza_number = dat['poliza_number'], start_of_validity = i_vig,
                                                end_of_validity = f_vig,document_type = 3, org_name = org_name)           
                            except Exception as erCar:
                                print('ere',erCar)
                                error = 'La Póliza de Grupo no existe (NUMERO_POLIZA '+str(dat['poliza_number'])+')'
                                errores.append(error)                        
                        except Polizas.DoesNotExist:
                            error = 'No se encontro la Póliza de Grupo: '+str(dat['poliza_number'])
                            errores.append(error)
                        except Polizas.MultipleObjectsReturned:
                            # return Response(status=status.HTTP_404_NOT_FOUND, data = 'Existe más de un registro')
                            error = 'Existe más de un registro de la Póliza de Grupo: '+str(dat['poliza_number'])
                            errores.append(error)
                        if caratulaExists:
                            if caratulaExists.ramo.ramo_code == 2:                        
                                error = 'La Póliza de Grupo es de Accidentes y Enfermedades '+str(dat['poliza_number'])+', se editará correctamente'
                                errores.append(error)
                            else:                      
                                error = 'La Póliza de Grupo NO es de Accidentes y Enfermedades '+str(dat['poliza_number'])+', NO APLICA EL CAMBIO'
                                errores.append(error)                    
                        else:     
                            error = 'No existe la Póliza de Grupo: '+str(dat['poliza_number'])
                            errores.append(error) 
    
        if len(errores) == 0:
            error = 'Layout correcto, puede proceder a Guardar la Información Contributoria de las Pólizas de Grupo'
            errores.append(error)

    if post.get('validar') == False or post.get('validar') == 'False':   
        results = []
        try:
            for certs in polizasToEdit:     
                certs = certs.replace("'", '"')
                try:
                    certs = json.loads(certs)  
                except:
                    certs = eval(certs)  
                for dt in certs:
                    if dt['start_of_validity'] and dt['end_of_validity']:
                        try:
                            f = "%d/%m/%Y"        
                            i_vig = datetime.strptime(dt['start_of_validity'] , f)
                            f_vig = datetime.strptime(dt['end_of_validity'] , f)
                        except:
                            f = "%m/%d/%Y"        
                            i_vig = datetime.strptime(dt['start_of_validity'] , f)
                            f_vig = datetime.strptime(dt['end_of_validity'] , f)
                    caratulaExists = None
                    if (dt['id'] and (dt['id'] != 0 or dt['id'] != '0')):
                        try:    
                            try:            
                                caratulaExists = Polizas.objects.get(pk = dt['id'], document_type = 3, org_name = org_name)           
                            except Exception as erCar:
                                pass                      
                        except Polizas.DoesNotExist:
                            pass
                        except Polizas.MultipleObjectsReturned:
                            pass
                    else:
                        if (dt['poliza_number'] != '' and dt['start_of_validity'] != '' and dt['end_of_validity'] != ''):
                            try:    
                                try:            
                                    caratulaExists = Polizas.objects.get(poliza_number = dt['poliza_number'], start_of_validity = i_vig,
                                                    end_of_validity = f_vig,document_type = 3, org_name = org_name)           
                                except Exception as erCar:
                                    pass
                            except Exception as erCar:
                                    pass  
                    if caratulaExists:
                        if caratulaExists.ramo:
                            if caratulaExists.ramo.ramo_code ==2:
                                try:
                                    caratulaExists.rfc_cve = dt['rfc_cve']
                                    caratulaExists.rfc_homocve = dt['rfc_homocve']
                                    caratulaExists.dom_callenum = dt['dom_callenum']
                                    caratulaExists.dom_colonia = dt['dom_colonia']
                                    caratulaExists.dom_cp = dt['dom_cp']
                                    caratulaExists.dom_poblacion = dt['dom_poblacion']
                                    caratulaExists.dom_estado = dt['dom_estado']
                                    caratulaExists.contributory = True
                                    caratulaExists.save()
                                    print('--caratula grupo---',caratulaExists.id, caratulaExists.contributory)
                                    # LOG certificado
                                    dataIdent = ' edito Póliza de Grupo : '+str(caratulaExists.poliza_number)
                                    change= {}                        
                                    error = 'Póliza de Grupo editada '+str(caratulaExists.poliza_number)
                                    errores.append(error)
                                    try:
                                        send_log_complete(request.user, org_name, 'POST', 18, '%s' % str(dataIdent),'%s' % str(original),'%s' % str(change), caratulaExists.id)
                                    except Exception as eee:
                                        pass
                                except Exception as e:
                                    error = 'Error al editar Póliza de Grupo '+str(e)
                                    print('erorr----',e)
                                    errores.append(e)
                            else:
                                error = 'Error al editar Póliza de Grupo No es de Accidentes y Enfermedades '+str(caratulaExists.poliza_number)
                                errores.append(error)
                    else:                        
                        error = 'Error al editar Póliza de Grupo, no existe ' +str(dt['id'] if dt['id'] else dt['poliza_number'])
                        errores.append(error)

            if len(errores) == 0:
                error = 'Layout correcto, Póliza de Grupos creados'
                errores.append(error)
        except Exception as e:
            print('-err',e)
            error = 'Error al editar Pólizas de Grupo '+str(e)
            errores.append(error)
    if post.get('validar') == True or post.get('validar') == 'True':
        df = pd.DataFrame({
        'Retroalimentación de validación de pgrupo': errores,
        } )
        showtime = str(strftime("%Y_%m_%d_%H_%M_%S", gmtime()))
        filename = 'reporte_validacionpgrupo_%s_%s_%s.xlsx'%(org_name, self.request.id,showtime)
    else:
        df = pd.DataFrame({
        'Retroalimentación de Edición de PGrupo Info Contributoria': errores,
        } )
        showtime = str(strftime("%Y_%m_%d_%H_%M_%S", gmtime()))
        filename = 'reporte_ediciioninfocontribpgrupo_%s_%s_%s.xlsx'%(org_name, self.request.id,showtime)
    writer = pd.ExcelWriter(filename, engine='xlsxwriter')
    # Convert the dataframe to an XlsxWriter Excel object.
    df.to_excel(writer, sheet_name='Sheet1', index=False)
    # Close the Pandas Excel writer and output the Excel file.
    writer.save()
    url_response = 'timeout'
    with open(filename) as f:
        url_response = upload_to_s3(filename, org_name)
        try:
            try:
                user_ownern = post.get('user_req')
                user_owner = User.objects.get(username = user_ownern)
            except Exception as uname:
                user_ownern = 'superuser_'+org_name
                user_owner = User.objects.get(username = user_ownern)
            # crear lanotificación
            payload = {
                "seen": False,
                "id_reference":0,
                "assigned":user_ownern,
                "org_name":org_name,
                "owner":user_ownern,
                "description":url_response,
                "model":26,
                "title":'El reporte (retroalimentación infocontributoria) solicitado ha sido generado'
            }
            r = requests.post(settings.API_URL + 'crear-notificacion-reportes/', (payload), verify=False)
        except Exception as er:
            print('no se creo al notificacion retroalimentación infocontributoria',er)
            pass
        print(url_response)
        redis_client.publish(self.request.id, url_response)  
        os.remove(filename)        
    return url_response

def getStates(data):
    status = {
        1 : 'Aguascalientes',
        2 : 'Baja California',
        3 : 'Baja California Sur',
        4 : 'Campeche',
        5 : 'Chiapas',
        6 : 'Chihuahua',
        7 : 'Coahuila',
        8 : 'Colima',
        9 : 'Ciudad de México',
        10: 'Durango',
        11: 'Estado de México',
        12: 'Guanajuato',
        13: 'Guerrero',
        14: 'Hidalgo',
        15: 'Jalisco',
        16: 'Michoacán',
        17: 'Morelos',
        18: 'Nayarit',
        19: 'Nuevo León',
        20: 'Oaxaca',
        21: 'Puebla',
        22: 'Querétaro',
        23: 'Quintana Roo',
        24: 'San Luis Potosí',
        25: 'Sinaloa',
        26: 'Sonora',
        27: 'Tabasco',
        28: 'Tamaulipas',
        29: 'Tlaxcala',
        30: 'Veracruz',
        31: 'Yucatán',
        32: 'Zacatecas',
    }
    return status.get(data, '')

def getInfoOrg(request):
    org_ = requests.get(settings.CAS2_URL + 'get-org-info/'+request['org'])
    response_org= org_.text
    org_data = json.loads(response_org)
    org_info = org_data['data']['org']
    return org_info


from control.models import Session
def comisions(request):
    # token  = request['token']
    # session_obj = Session.objects.get(jwt_token=token)
    # permissions = session_obj.permissions['SAAM']
    # for permission in permissions['Comisiones']:
    #     if (permission['name']=="Comisiones" and permission['checked']):
    #         return True
    #     else:
    #         if permission['name']=="Comisiones":
    #             return permission['checked']
    return True

def life_type(self, value):
    if int(value) == 47:
        value = 'Ahorro'
    elif int(value) == 48:
        value = 'Vitalicia'
    elif int(value) == 49:
        value = 'Temporal/Protección'
    return value
def accidentes_type(self, value):
    if int(value) == 46:
        value = 'Familiar'
    elif int(value) == 64:
        value = 'Viajero'
    return value

import ast
from paquetes.models import Package
redis_client = redis.Redis(host='localhost', port=6379, db=0)
@app.task(bind=True, queue=settings.QUEUE)
def reporte_polizas_asincrono(self,post,otropost):
    org = post['org']
    # --------Parámetros----
    provider = post['providers']
    ramo = post['ramos']
    subramo = post['subramos'] 
    report_by = post['report_by']   
    since = post['since']
    until = post['until']
    # status = post['status']
    payment = post['payment']
    contratante = post['contratante']
    group = post['grupo']
    cve = post['cve']
    type_person = post['type_contractor']
    ot_rep = post['ot_rep'] if 'ot_rep' in post else 3
    order = int(post['order'])
    sucursal = int(post['sucursal'])
    asc = int(post['asc'])

    identifier = post['identifier']
    poliza_n = post['poliza']
    excel_type = int(post['excel_type'] if 'excel_type' in post else 2)
    group_by = int(post['group_by'])
    add_renovadas = int(post['renewals'])
    valDolar = post['valDolar']
    valUdi = post['valUdi']
    # --------****
    subgrupo = post['subgrupo']
    subsubgrupo = post['subsubgrupo']
    nivelagrupacion = post['groupinglevel']
    subnivel = post['subgrupinglevel']
    subsubnivel = post['subsubgrupinglevel']
    bsLine = post['businessLine']
    clasificacion = post['classification']
    celula = post['celula']
    verReferenciadores = post.get('verReferenciadores', True)

    try:
        ramos_sel = ast.literal_eval(post['ramos'])
    except:
        ramos_sel = None
    try:
        subramos_sel = ast.literal_eval(post['subramos'])
    except:
        subramos_sel =None

    try:
        providers_sel = ast.literal_eval(post['providers'])
    except:
        providers_sel = None

    try:
        users_sel = ast.literal_eval(post['users'])
    except:
        users_sel = None

    try:
        onlyCaratula = post['only_caratula']
    except:
        onlyCaratula = 0
    # --------****
    # oriegn 1: nueva; 2 renovación; 0 ambas
    try:
        origen = int(post['origin'])
    except Exception as eee:
        origen = 0
    # ········ Inician filtros ········

    # Filtro de identificador
    if identifier != '0':
        identif = identifier
    else:
        identif = ''

    # Filtro de número de póliza
    if poliza_n != '0':
        npolicy = poliza_n
    else:
        npolicy = ''
    
    # Filtro de forma de pago
    if int(payment) > 0:
            fp = [int(payment)]
    else:
            fp = [12,24,6,5,4,3,2,1,7,14,15]

    # Filtro de status
    try:
        status = ast.literal_eval(post['status'])
    except:
        status = None

    try:
        if (status):
            st = status
        else:
            st = [1,2,4,10,11,12,13,14,15]
    except Exception as e:
        st = [1,2,4,10,11,12,13,14,15]

    # if int(status) > 0:
    #         st = [int(status)]
    # else:
    #         st = [1,2,4,10,11,12,13,14,15]
    if ot_rep == 3: #no ots
        pos = 0
        while pos<len(st):
            if st[pos]==1:
                st.pop(pos)
            if st[pos]==2:
                st.pop(pos)
            else:
                pos=pos+1
    # Filtro de grupo
    if int(group) > 0:
        grupos1 = Group.objects.get(pk = int(group))
        subg = Group.objects.filter(parent__id = int(group), type_group = 2, org_name=post['org']).values_list('pk', flat=True)
        subsubg = Group.objects.filter(parent__id__in = subg, type_group = 3, org_name=post['org']).values_list('pk', flat=True)
        allgrupos = list(subg) + list(subsubg)
        allgrupos.append(grupos1.id)
        grupos = Group.objects.filter(pk__in = allgrupos, org_name=post['org']).values_list('pk', flat=True)
    else:
        grupos = Group.objects.filter(org_name=post['org']).values_list('pk', flat=True)

    if int(subgrupo) > 0:
        subg = Group.objects.get(pk = int(subgrupo), org_name=post['org'])
        subsubg = Group.objects.filter(parent__id = int(subgrupo), type_group = 3, org_name=post['org']).values_list('pk', flat=True)
        allgrupos =list(subsubg)
        allgrupos.append(subg.id)
        grupos = Group.objects.filter(pk__in = allgrupos, org_name=post['org'])

    if int(subsubgrupo) > 0:
        grupos = Group.objects.get(pk = int(subsubgrupo), org_name=post['org'])


    if int(clasificacion) > 0:
        clasifics = Classification.objects.get(pk = int(clasificacion), org_name=post['org'])
    else:
        clasifics = Classification.objects.filter(org_name=post['org'])


    # Filtro de aseguradora
    if providers_sel and providers_sel != '0':
            providers = list(Provider.objects.filter(pk__in = (providers_sel)).values_list('pk', flat=True))
    else:
            providers = list(Provider.objects.filter(org_name = post['org']).values_list('pk', flat=True))

    # Filtro de ramo
    if ramos_sel and ramos_sel != '0':
            ramos = list(Ramos.objects.filter(org_name = post['org'], provider__in = providers,ramo_code__in=ramos_sel).values_list('pk', flat=True))
    else:
            ramos = list(Ramos.objects.filter(org_name = post['org'], provider__in = providers).values_list('pk', flat=True))
    # Filtro de subramo
    if subramos_sel and subramos_sel != '0':
            subramos = list(SubRamos.objects.filter(org_name = post['org'], ramo__in = ramos,subramo_code__in= subramos_sel).values_list('pk', flat=True))
    else:
            subramos = list(SubRamos.objects.filter(org_name = post['org'], ramo__in = ramos).values_list('pk', flat=True))   
    
    # Filtro de usuario
    if users_sel and users_sel != '0':
            users = list(User.objects.filter(pk__in = (users_sel)).values_list('pk', flat=True))
    else:
            users = list(User.objects.values_list('pk', flat=True))

    # Filtro de clave
    clave = 0
    try:
        clave = int(cve)
        if clave > 0:
            cves = list(Claves.objects.filter(clave__icontains = cve, org_name = post['org']).values_list('pk', flat=True))
        else:
            cves = list(Claves.objects.filter(org_name = post['org']).values_list('pk', flat=True))
    except:
        cves = list(Claves.objects.filter(clave__icontains = cve, org_name = post['org']).values_list('pk', flat=True))
    


    if npolicy:
        polizas = Polizas.objects.filter(status__in = st, 
                                                                     forma_de_pago__in = fp, 
                                                                     org_name=post['org'],
                                                                     ramo__in = ramos, 
                                                                     subramo__in = subramos, 
                                                                     aseguradora__in = providers,
                                                                     # clave__in = cves,
                                                                     owner__in = users,
                                                                     poliza_number__icontains = npolicy)
    elif identif:
        polizas = Polizas.objects.filter(status__in = st, 
                                                                     forma_de_pago__in = fp, 
                                                                     org_name=post['org'],
                                                                     ramo__in = ramos, 
                                                                     subramo__in = subramos, 
                                                                     aseguradora__in = providers,
                                                                     # clave__in = cves,
                                                                     owner__in = users,
                                                                     identifier__icontains = identif)
    else:
        polizas = Polizas.objects.filter(status__in = st, 
                                                                     forma_de_pago__in = fp, 
                                                                     org_name=post['org'],
                                                                     ramo__in = ramos, 
                                                                     subramo__in = subramos, 
                                                                     aseguradora__in = providers,
                                                                     # clave__in = cves,
                                                                     owner__in = users)
    
    if clave > 0:
        polizas = polizas.filter(clave__in = cves)
    # Filtro de contratante
    if int(contratante) > 0 :
        contratanten = list(Contractor.objects.filter(pk = int(contratante), group__in = grupos).values_list('pk', flat = True))
        polizas = polizas.filter(contractor__in = contratanten)
    else:
        contratanten = list(Contractor.objects.filter(group__in = grupos).values_list('pk', flat = True))
        polizas = polizas.filter(contractor__in = contratanten)
    # ----------------------******************************---------------------------------------------------
    if int(group) > 0 :
        # polizas = polizas.filter(Q(natural__group = grupos) | Q(juridical__group = grupos))
        carat = polizas.filter(document_type__in = [1,3,11])
        carat = list(carat.filter(contractor__group__in = grupos).values_list('pk', flat = True))
        # pol = polizas.filter(document_type = 12).values_list('pk', flat = True)
        only_caratulas = Polizas.objects.filter(pk__in = carat,document_type = 11)
        pol = list(polizas.filter(document_type = 12, parent__in = only_caratulas).values_list('pk', flat = True))
        polizas = carat + pol
        polizas = Polizas.objects.filter(pk__in = polizas)

    # -----------------------------***********************************************-----------------------------------   
    if int(clasificacion) > 0 :
        polizas = polizas.filter(contractor__classification = clasifics)
    if int(celula) > 0:
        polizas = polizas.filter(celula = celula)

    gp = nivelagrupacion
    gp1= subnivel
    gp2 = subsubnivel

    try:
        gp = int(gp)
    except:
        pass
    try:
        gp1 = int(gp1)
    except:
        pass
    try:
        gp2 = int(gp2)
    except:
        pass
    


    if gp > 0 or gp1 > 0 or gp2 > 0:
        
        if(int(gp) == 0):
            nivelagrupacion = GroupingLevel.objects.filter(org_name = post['org'], type_grouping = 1)
            subnivel = GroupingLevel.objects.filter(org_name = post['org'], type_grouping = 2, parent__in = list(nivelagrupacion))
            subsubnivel = GroupingLevel.objects.filter(org_name = post['org'], type_grouping = 3, parent__in = list(subnivel))
        else :
            nivelagrupacion = GroupingLevel.objects.filter(id = gp)
            subnivel = GroupingLevel.objects.filter(org_name = post['org'], type_grouping = 2, parent__in = list(nivelagrupacion))
            subsubnivel = GroupingLevel.objects.filter(org_name = post['org'], type_grouping = 3, parent__in = list(subnivel))
        if(int(gp1) == 0):
            subnivel = subnivel
        else :
            subnivel = subnivel.filter(id = gp1)
        if(int(gp2) == 0):
            subsubnivel = subsubnivel
        else :
            subsubnivel = subsubnivel.filter(id = gp2)

        if int(gp2) >0:
            niveles = list(subsubnivel.filter(parent__id = gp1))
        elif int(gp1) >0:
            niveles = list(subnivel.filter(parent__id = gp))+list(subsubnivel.filter(parent__id = gp1))
        else:
            niveles = list(subsubnivel) + list(subnivel) + list(nivelagrupacion)

        polizas = polizas.filter(groupinglevel__in = niveles)
    
    if int(bsLine) > 0:
        if int(bsLine) == 3:
            polizas = polizas.filter(business_line = 0) 
        else:
            polizas = polizas.filter(business_line = bsLine)
    # -----------------------------***********************************************-----------------------------------
    # Filtro de renovadas
    if add_renovadas == 1:
        polizas = polizas.filter(renewed_status=1)
    elif add_renovadas == 2:
        polizas = polizas.filter(renewed_status=0)
    elif add_renovadas == 0:
        polizas = polizas

    

    # Filtro de fechas
    try:
        f = "%d/%m/%Y %H:%M:%S"        
        since = datetime.strptime(since , f)
        until = datetime.strptime(until , f)

    except:
        f = "%m/%d/%Y %H:%M:%S"        
        since = datetime.strptime(since , f)
        until = datetime.strptime(until , f)


    if int(report_by) == 2:
        date_filters = [Q(start_of_validity__gte=since),Q(start_of_validity__lte = until)]
    elif int(report_by) == 1:
        date_filters = [Q(created_at__gte=since),Q(created_at__lte = until), Q(migrated = False)]
    # con cobertura aún siendo renovadas, vencidas 
    elif int(report_by) == 3:
        now = timezone.now()
        start_of_tomorrow = timezone.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        date_filters = [Q(end_of_validity__gte=start_of_tomorrow)]
        st = [x for x in st if x not in (10, 11, 2, 4)]
        polizas = polizas.filter(status__in =st).filter(end_of_validity__gte=start_of_tomorrow)
    else:
        date_filters = [Q(start_of_validity__gte=since),Q(start_of_validity__lte = until)]


    # print('ot', ot_rep)
    if ot_rep and int(ot_rep) == 1:
        polizas = polizas.filter(reduce(operator.and_, date_filters), document_type__in = list([1,3,11,12]) ,org_name = post['org'], status__in = [1,2])
    elif ot_rep and int(ot_rep) == 2:
        polizas = polizas.filter(reduce(operator.and_, date_filters), document_type__in = list([1,3,11,12]) ,org_name = post['org'])
    elif ot_rep and int(ot_rep) == 3:
        polizas = polizas.filter(reduce(operator.and_, date_filters), document_type__in = list([1,3,11,12]) ,org_name = post['org']).exclude(status__in = [1,2])
    else:
        polizas = polizas.filter(reduce(operator.and_, date_filters), document_type__in = list([1,3,11,12]) ,org_name = post['org']).exclude(status__in = [1,2])
    
    
    vendedor = post['vendedor']
    if vendedor != 0 and vendedor != '0':
        refs = ReferenciadoresInvolved.objects.filter(
            referenciador = vendedor,
            org_name =  post['org'],is_changed=False
        ).values_list('policy', flat=True)
        polizas = polizas.filter(id__in = list(refs))


    else:
        polizas = polizas
    if sucursal != 0:
        polizas = polizas.filter(sucursal__id = sucursal)
    if int(onlyCaratula) != 0:
        polizas = polizas.filter(Q(id=onlyCaratula) | Q(parent__id=onlyCaratula), document_type__in = list([11,12]))
    else:
        polizas = polizas
    # Try
    org_name = post['org']
    polizasK = polizas.values_list('pk',flat = True)
    old_policies = OldPolicies.objects.filter(org_name = org)
    polizasEvaluateN=list(old_policies.values_list('new_policy', flat=True))
    if origen == 1:
        interseccion = Polizas.objects.filter(id__in = polizasK).filter(id__in = polizasEvaluateN )
        polizas = Polizas.objects.filter(id__in = polizasK).exclude(id__in = interseccion)
    elif origen == 2:
        polizasR_ = Polizas.objects.filter(pk__in=polizasEvaluateN)
        polizasR_ = polizasR_.filter(pk__in = polizasK)
        polizas = polizasR_
    else:
        interseccion = Polizas.objects.filter(id__in = polizasK).filter(id__in = polizasEvaluateN )
        polizasNew = Polizas.objects.filter(id__in = polizasK).exclude(id__in = interseccion)
        polizasNewAdd1 = polizasNew
        # 
        polizasR_ = Polizas.objects.filter(pk__in=polizasEvaluateN)
        polizasR_ = polizasR_.filter(pk__in = polizasK)
        polizasNewAdd2 = polizasR_
    # xxxxxxxx
    polizas_enproceso = Polizas.objects.filter(
        org_name =post['org'], status__in=[10,11,13,14], renewed_status__in=[2]
    )
    # xxxxxxx
    try:
        dataToFilter = getDataForPerfilRestricted(post['user'], post['org'])
    except Exception as er:
        dataToFilter = {}
    if dataToFilter:
        # Contratantes***
        polizasCl = Polizas.objects.filter(document_type__in = [1,3,11,7,8,12,6,4], org_name = post['org'])
        polizasToF = Polizas.objects.filter(document_type__in = [1,3,11,7,8], org_name = post['org'])
        if dataToFilter['ccpr']:
            polizasToF = polizasToF.filter(contractor__in = list(dataToFilter['ccpr']))
        if dataToFilter['cgpr']:
            polizasToF = polizasToF.filter(contractor__group__in = list(dataToFilter['cgpr']))
        if dataToFilter['ccepr']:
            polizasToF = polizasToF.filter(contractor__cellule__in = list(dataToFilter['ccepr']))
        if dataToFilter['crpr']:
            polizasToF = polizasToF.filter(contractor__vendor__in = list(dataToFilter['crpr']))
        if dataToFilter['cspr']:
            polizasToF = polizasToF.filter(contractor__sucursal__in = list(dataToFilter['cspr']))
        # Pólizas ****
        if dataToFilter['pppr']:
            polizasToF = polizasToF.filter(pk__in = list(dataToFilter['pppr']))
        if dataToFilter['pgpr']:
            polizasToF = polizasToF.filter(contractor__group__in = list(dataToFilter['pgpr']))
        if dataToFilter['pcepr']:
            polizasToF = polizasToF.filter(celula__in = list(dataToFilter['pcepr']))
        if dataToFilter['prpr']:
            polizasToF = polizasToF.filter(ref_policy__referenciador__in = list(dataToFilter['prpr']))
        if dataToFilter['pspr']:
            polizasToF = polizasToF.filter(sucursal__in = list(dataToFilter['pspr']))
        if dataToFilter['papr']:
            polizasToF = polizasToF.filter(groupinglevel__in = list(dataToFilter['papr']))
        if dataToFilter['pcapr']:
            polizasToF = polizasToF.filter(clave__in = list(dataToFilter['pcapr']))
        if dataToFilter['psrpr']:
            polizasToF = polizasToF.filter(subramo__subramo_code__in = list(dataToFilter['psrpr']))
        if dataToFilter['paspr']:
            polizasToF = polizasToF.filter(aseguradora__in = list(dataToFilter['paspr']))
        if dataToFilter['pstpr']:
            polizasToF = polizasToF.filter(status__in = list(dataToFilter['pstpr']))
        
        polizasCT = polizasCl.filter(document_type = 12, parent__in = list(polizasToF))
        polizasGT = polizasCl.filter(document_type = 6, parent__parent__parent__in = list(polizasToF))
        polizasFin = list(polizasToF.values_list('pk', flat = True)) + list(polizasCT.values_list('pk', flat = True)) + list(polizasGT.values_list('pk', flat = True))
        polizas = polizas.filter(pk__in = list(polizasFin)) 
        polizas_enproceso=polizas_enproceso.filter(pk__in = list(polizasFin)) 
    # ········ Inicia Excel ········  
    # Empieza insertado de imagen
    try:
        info_org = getInfoOrg(post)
        if len(info_org['logo']) != 0:
            archivo_imagen = 'https://miurabox-public.s3.amazonaws.com/cas/' + info_org['logo']
        else:
            archivo_imagen = 'saam.jpg'
    except Exception as d:
        archivo_imagen = 'saam.jpg'
    tipo_reporte = "Reporte de pólizas del " + str(since.strftime("%d/%m/%y")) + " al " + str(until.strftime("%d/%m/%y"))
    row_num = 10 # A partir de aqui pinta la tabla de resultados

    # Permiso de ver comisión
    si_com = comisions(post)
    try:
        polizas_pesos = polizas.filter(f_currency =1)
        p_neta_total_pesos = polizas_pesos.aggregate(Sum('p_neta'))
        comision_total_pesos = polizas_pesos.aggregate(Sum('comision'))
        p_total_total_pesos = polizas_pesos.aggregate(Sum('p_total'))

        polizas_dolares = polizas.filter(f_currency =2)
        p_neta_total_dolares = polizas_dolares.aggregate(Sum('p_neta'))
        comision_total_dolares = polizas_dolares.aggregate(Sum('comision'))
        p_total_total_dolares = polizas_dolares.aggregate(Sum('p_total'))

        polizas_udi = polizas.filter(f_currency =3)
        p_neta_total_udi = polizas_udi.aggregate(Sum('p_neta'))
        comision_total_udi = polizas_udi.aggregate(Sum('comision'))
        p_total_total_udi = polizas_udi.aggregate(Sum('p_total'))
        sumas = {
            'p_total_total_pesos': p_total_total_pesos['p_total__sum'],
            'p_neta_total_pesos': p_neta_total_pesos['p_neta__sum'],
            'comision_total_pesos': comision_total_pesos['comision__sum'],
            'p_total_total_dolares': p_total_total_dolares['p_total__sum'],
            'p_neta_total_dolares': p_neta_total_dolares['p_neta__sum'],
            'comision_total_dolares': comision_total_dolares['comision__sum'],
            'p_total_total_udi': p_total_total_udi['p_total__sum'],
            'p_neta_total_udi': p_neta_total_udi['p_neta__sum'],
            'comision_total_udi': comision_total_udi['comision__sum']
        }
        if sumas['p_total_total_dolares'] == None:
            sumas['p_total_total_dolares'] = float(0)
        if sumas['p_neta_total_dolares'] == None:
            sumas['p_neta_total_dolares'] = float(0)
        if sumas['comision_total_dolares'] == None:
            sumas['comision_total_dolares'] = float(0)

        if sumas['p_total_total_udi'] == None:
            sumas['p_total_total_udi'] = float(0)
        if sumas['p_neta_total_udi'] == None:
            sumas['p_neta_total_udi'] = float(0)
        if sumas['comision_total_udi'] == None:
            sumas['comision_total_udi'] = float(0)
    except Exception as e:
        sumas = {}
    if float(valDolar) == 0.00:
            sumas = sumas
    else:
        try:
            if sumas['p_total_total_dolares'] != None:
                    sumas['p_total_total_dolares'] = float(sumas['p_total_total_dolares']) * float(valDolar)
            else:
                sumas['p_total_total_dolares'] = float(0)

            if sumas['p_neta_total_dolares'] != None:
                    sumas['p_neta_total_dolares'] = float(sumas['p_neta_total_dolares']) * float(valDolar)
            else:
                sumas['p_neta_total_dolares'] = float(0)

            if sumas['comision_total_dolares'] != None:
                sumas['comision_total_dolares'] =float(sumas['comision_total_dolares']) * float(valDolar)
            else:
                sumas['comision_total_dolares'] = float(0)

        except Exception as e:
                sumas['p_total_total_dolares'] = sumas['p_total_total_dolares']
                sumas['p_neta_total_dolares'] = sumas['p_neta_total_dolares']
                sumas['comision_total_dolares'] = sumas['comision_total_dolares']
    if float(valUdi) == 0.00:
            sumas = sumas
    else:
        try:
            if sumas['p_total_total_udi'] != None:
                    sumas['p_total_total_udi'] = float(sumas['p_total_total_udi']) * float(valUdi)
            else:
                sumas['p_total_total_udi'] = float(0)
            if sumas['p_neta_total_udi'] != None:
                    sumas['p_neta_total_udi'] = float(sumas['p_neta_total_udi']) * float(valUdi)
            else:
                sumas['p_neta_total_udi'] = float(0)
            if sumas['comision_total_udi'] != None:
                    sumas['comision_total_udi'] = float(sumas['comision_total_udi']) * float(valUdi)
            else:
                sumas['comision_total_udi'] = float(0)
        except Exception as e:
            sumas['p_total_total_udi'] = sumas['p_total_total_udi']
            sumas['p_neta_total_udi'] = sumas['p_neta_total_udi']
            sumas['comision_total_udi'] = sumas['comision_total_udi']
    # Estilo General
    # Excel agrupado
    if excel_type == 1:
        group_by = post.get('group_by')        
        if int(group_by) == 1 or int(group_by) == 2 or int(group_by) == 3:
            if int(group_by) == 1:
                print('recibos_por_aseguradora')
                rows_pesos, rows_dolares, rows_pesosf, rows_dolaresf,rows_udi,rows_udif = polizas_por_aseguradora(polizas)

            elif int(group_by) == 2:
                print('recibos_por_ramo')
                rows_pesos, rows_dolares, rows_pesosf, rows_dolaresf,rows_udi,rows_udif = polizas_por_ramo(polizas)

            elif int(group_by) == 3:
                print('polizas_por_subramo')
                rows_pesos, rows_dolares, rows_pesosf, rows_dolaresf,rows_udi,rows_udif = polizas_por_subramo(polizas)            
            return crea_reporte_agrupado_1p(rows_pesos, rows_dolares, rows_pesosf, rows_dolaresf,rows_udi,rows_udif, post, self.request.id, redis_client,polizas)

        elif int(group_by) == 4:

            rows_pesos, rows_dolares, rows_pesosf, rows_dolaresf,rows_udi,rows_udif = polizas_por_grupo(polizas)
            return crea_reporte_agrupado_2p(rows_pesos, rows_dolares, rows_pesosf, rows_dolaresf,rows_udi,rows_udif, post, self.request.id, redis_client,polizas)

        elif int(group_by) == 5:
            rows_pesos, rows_dolares,rows_udi = polizas_por_referenciadores(polizas)
            return crea_reporte_agrupado_3p(rows_pesos, rows_dolares,rows_udi, post, self.request.id, redis_client,polizas)
        
        elif int(group_by) == 6:
            rows_pesos, rows_dolares, rows_udi = polizas_por_estatus(polizas)
            return crea_reporte_agrupado_5p(rows_pesos, rows_dolares,rows_udi, post, self.request.id, redis_client,polizas)
            
        elif int(group_by) == 7:
            rows_pesos, rows_dolares, rows_pesosf, rows_dolaresf,rows_udi,rows_udif  = polizas_por_owner(polizas)
            return crea_reporte_agrupado_4p(rows_pesos, rows_dolares, rows_pesosf, rows_dolaresf,rows_udi,rows_udif , post, self.request.id, redis_client,polizas) 
        elif int(group_by) == 8:
            rows_pesos, rows_dolares, rows_pesosf, rows_dolaresf,rows_udi,rows_udif = polizas_por_contratante(polizas)
            return crea_reporte_agrupado_2p(rows_pesos, rows_dolares, rows_pesosf, rows_dolaresf,rows_udi,rows_udif, post, self.request.id, redis_client,polizas)                
        # ---------report service excel----
    # Excel no agrupado
    else:
        columns = ['Tipo', 'No.Póliza', 'No. Interno', 'Contratante','Grupo', 'Email', 'Teléfono', 'Proveedor', 'Subramo','Tipo(por subramo)',
            'Forma de Pago', 'Estatus', 'Inicio Póliza', 'Fin Póliza', 'Moneda', 'Prima Neta', 'Descuento',
            'RPF', 'Derecho', 'IVA', 'Prima Total', 'Comisión','%Comisión Prima Neta','%Comisión RPF','%Comisión Derecho', 'Clave', 'Sucursal', 'Referenciador', 'Creado por',
            'Fecha creación', 'Observaciones', 'Asegurado', 'Marca', 'Modelo', 'Versión', 'Año',
            'Placas', 'Motor','Procedencia', 'Adaptaciones', 'Conductor','Dirección','Código Postal','Identificador','Ejecutivo Cobranza',
            'Responsable','Ramo','Cancelacion','Renovable','OT/Póliza asociada','Origen Póliza','Paquete', 'Subgrupo','Subsubgrupo',
            'Agrupación','Subagrupación','Subsubagrupación','Clasificación','Línea Negocio','Célula','Fecha Cancelación','Adjuntos',
            'Colectividad/PG completa','Conducto de Pago','RFC de Cliente','Seguimiento']
            
        try:
            columns =ast.literal_eval(post['cols1'])
        except Exception as e:
            print('--errr',columns,e)
            pass        
        # ******************************************
        if 'Clave' in columns:
            indx = columns.index('Clave')
            columns[indx] = 'NOMBRE DEL AGENTE'  
            columns.insert(indx + 1, 'CLAVE DEL AGENTE')  

        # Empieza insertado de imagen
        # columns.insert(3,'Email')
        columns.append('ID_SAAM')
        # columns.append('Conducto de Pago')
        # columns.append('Contributoria')
        campo_celula, campo_agrupacion, campo_lineanegocio, moduleName = getOrgInfo(post['org'])
        if campo_celula == 'False' or campo_celula ==False:
            if 'Célula' in columns:
                columns.remove('Célula')
        else:
            if 'Célula' in columns:
                index = columns.index('Célula')
                columns[index] = moduleName  # reemplaza por el valor
        if campo_agrupacion == 'False' or campo_agrupacion ==False:
            if 'Agrupación' in columns:
                columns.remove('Agrupación')
            if 'Subagrupación' in columns:
                columns.remove('Subagrupación')
            if 'Subsubagrupación' in columns:
                columns.remove('Subsubagrupación')
        if campo_lineanegocio == 'False' or campo_lineanegocio ==False:
            if 'Línea de Negocio' in columns:
                columns.remove('Línea de Negocio')
        # if not 'Seguimiento' in columns:
        #     columns.append('Seguimiento')
        index = columns.index('ID_SAAM')
        # Insert a new element before 'ID SAAM'
        # if 'RFC de Cliente' not in columns:
        #     columns.insert(index, 'RFC de Cliente')
        try:
            index2 = columns.index('Motor')
            if 'Procedencia' not in columns:
                columns.insert(index2+1, 'Procedencia')
        except Exception as ec:
            print('error',ec)
        # if 'Comisión Porcentaje' in columns or '%Comisión Prima Neta' in columns:
        #     index3 = columns.index('Comisión Porcentaje')
        #     if 'Comisión Porcentaje' in columns:
        #         idx = columns.index('Comisión Porcentaje') 
        #         columns[idx] = '%Comisión Prima Neta'
        #     if not index3:
        #         index3 = columns.index('%Comisión Prima Neta')            
        #     columns.insert(index3+1, '%Comisión RPF')
        #     columns.insert(index3+2, '%Comisión Derecho')
        try:
            info_org = getInfoOrg(post)
            if len(info_org['logo']) != 0:
              archivo_imagen = 'https://miurabox.s3.amazonaws.com/cas/' + info_org['logo']
            else:
              archivo_imagen = 'saam.jpg'
        except Exception as d:
            archivo_imagen = 'saam.jpg'
        tipo_reporte = "Reporte de pólizas del " + str(since.strftime("%d/%m/%y")) + " al " + str(until.strftime("%d/%m/%y"))
        asegurado = []
        antiguedad = []
        referenciador = []
        creadopor = []
        dateRen = []
        otRen = []
        responsable = []
        contratante = []
        contratanteE = []
        contratanteP = []
        contratanteG = []
        collection = []
        clave = []
        clave_nombre = []
        cp = []
        caratulas = []
        paquete = []
        valueMA = []  
        valueModA = [] 
        valueVA = [] 
        valueYA = [] 
        valueLPA = [] 
        valueEA = [] 
        valueAdA = [] 
        valueDrA = []
        valueProcedencia = []
        tipo_admin = []
        nueva = []
        nivelAg = []
        grupo3 = []
        grupo4 = []
        clasifica = []
        poliza_siguiente = []
        poliza_anterior = []
        businessLine = []
        conducto_de_pago = []

        contactos_nombre = []
        contactos_email = []
        contactos_telefono = []
        celulaC = []
        cars_serials = []
        grupo1 = []       
        grupo2 = []
        fechac = []     
        adjuntos = []  
        comisionPercent = []  
        comisionDerecho = []  
        comisionRPF = []  
        completaPGC = []          
        edocir = []
        descuento = []
        correo_contratante = []
        ids = []
        polizas = polizas.order_by('id')
        bitacora = []
        contributoria=[]
        seguimiento=[]
        inicio = [] 
        fin = [] 
        statuspoliza = []
        tipo_por_subramo = []
        rfccliente=[]
        # polizas = polizas.order_by('natural__full_name','juridical__j_name')
        for r in polizas: 
            valuePr = ''
            tab = ''
            if r.document_type in [1,3,12]:
                if r.status ==1:
                    tablero =PromotoriaTablero.objects.filter(org_name=r.org_name)#config
                    if tablero:                        
                        confTab = tablero[0].polizas_ots
                        try:
                            confTab = json.loads(confTab)
                        except Exception as eee:
                            confTab = confTab
                            try:
                                confTab = eval(confTab)
                            except Exception as e:
                                pass
                        for ind,y in enumerate(confTab):
                            if r.id in y['polizas']:
                                tab=y['tablero']
                                break
                    else:
                        tab= ''
                else:
                    tab= ''
            else:
                tab= ''
            seguimiento.append(tab)
            contrib = 'No Aplica'
            if r.conducto_de_pago:
                conducto_de_pago.append(r.get_conducto_de_pago_display())
            else:
                conducto_de_pago.append('')

            comment = Comments.objects.filter(
                model = 1, 
                id_model = r.id,
                is_child = False
            )
            if comment.exists():
                comment = comment.last()
                bitacora.append(comment.content)
            else:
                bitacora.append('')

            if r.contractor and r.contractor.email:
                correo_contratante.append(r.contractor.email)
            else:
                correo_contratante.append('')
            if r.contractor and r.contractor.rfc:
                rfccliente.append(r.contractor.rfc)
            else:
                rfccliente.append('')
            ids.append(r.id)
            completa = ''
            comp = r.comision_percent
            comderecho = r.comision_derecho_percent
            comrpf = r.comision_rpf_percent
            adj = 'No'
            if PolizasFile.objects.filter(owner__id = r.id, org_name= r.org_name).exists():
                adj = 'Si'
            else:
                adj  ='No'

            if r.state_circulation:
                valstate = r.state_circulation
                try:
                    valstate = getStates(int(r.state_circulation))
                except:
                    valstate = r.state_circulation
            else:
                valstate = ''
            anterior = OldPolicies.objects.filter(new_policy = r)
            posterior = OldPolicies.objects.filter(base_policy = r)
            status_str = checkStatusPolicy(r.status)
            try:
                posterior_ots = posterior.filter(new_policy__status = 1)
                if r.renewed_status ==2 and posterior_ots:
                    status_str = status_str+'- En proceso de renovación'
                elif r.renewed_status ==2 and r.org_name =='ancora' and not posterior_ots:
                    status_str = status_str+'- En proceso de renovación'
                else:
                    status_str = status_str

            except Exception as dfs:
                print('error',dfs)
            fec = ''
            if r.status ==11:
                if r.date_cancel:
                    try:
                        fec = r.date_cancel
                        try:
                            formatted = r.date_cancel.strftime('%H/%M/%S')
                            # if formatted =='00/00/00' or formatted =='18/00/00' or formatted =='05/00/00':
                            end_date = fec+ timedelta(hours=6, minutes=00)
                            fec = end_date
                            fec = fec
                            fec = fec.strftime("%d/%m/%Y")
                        except Exception as eft:
                            fec = r.date_cancel
                    except:
                        fec = ''
                else:
                  fec = ''
            else:
              fec = ''
            if anterior.exists():
              anterior = anterior.last()
              poliza_anterior.append(anterior.base_policy.poliza_number)
            else:
              poliza_anterior.append('')

            if posterior.exists():
              posterior = posterior.last()
              try:
                poliza_siguiente.append(posterior.new_policy.poliza_number)
              except Exception as es:
                poliza_siguiente.append('')
            else:
              poliza_siguiente.append('')
            if r.document_type ==3 or r.document_type ==11:
                completa = 'Si'
                if r.document_type ==3:
                    try:
                        certs = Polizas.objects.filter(parent__parent__parent__id = r.id,document_type = 6, contributory = True,org_name =r.org_name).exclude(status = 0)
                        if certs:
                            contrib = 'Si'
                        else:
                            contrib = 'No'
                    except Exception as ee:
                        print('error contrb get certs',ee) 
                        contrib='No'

                    if (r.poliza_number ==None or r.poliza_number=='') or (r.receipts_by==1 and Recibos.objects.filter(poliza__id = r.id, receipt_type =1).exclude(status = 0).count() ==0):
                        completa = 'No'
                    if Polizas.objects.filter(parent__id = r.id, document_type =4).count() ==0:
                        completa = 'No'
                    if Polizas.objects.filter(parent__parent__id = r.id, document_type =5).count() ==0:
                        completa = 'No'
                    if Polizas.objects.filter(parent__parent__parent__id = r.id, document_type =6).count() ==0:
                        completa = 'No'
                    if Package.objects.filter(policy = r.id, type_package =2).count() ==0:
                        completa = 'No'
                    if (r.receipts_by==2 and Polizas.objects.filter(parent__id = r.id, document_type =4).exists()):
                        if Recibos.objects.filter(poliza__in = list(Polizas.objects.filter(parent__id = r.id, document_type =4)), receipt_type =1).exclude(status = 0).count() ==0:
                            completa = 'No'
                    if (r.receipts_by==3 and Polizas.objects.filter(parent__parent__parent__id = r.id, document_type =6).exists()):
                        if Recibos.objects.filter(poliza__in = list(Polizas.objects.filter(parent__parent__parent__id = r.id, document_type =6)), receipt_type =1).exclude(status = 0).count() ==0:
                            completa = 'No'
                if r.document_type ==11:
                    if (r.poliza_number ==None or r.poliza_number==''):
                        completa = 'No'
                    if Polizas.objects.filter(parent__id = r.id, document_type =12).count() ==0:
                        completa = 'No'
            if r.document_type == 12:
                try:
                    pn = r.parent.poliza_number
                except:
                    pn = ''
                caratulas.append(pn)
                if r.date_cancel:
                  if r.status ==11:
                    try:
                      fec = r.date_cancel.strftime("%d/%m/%Y") 
                    except:
                      fec = ''
                  else:
                    fec = ''
                else:
                  if r.status ==11:
                    try:
                      fec = r.parent.date_cancel.strftime("%d/%m/%Y") 
                    except:
                      fec = ''
                  else:
                    fec = ''
            else:
                caratulas.append('')
            if r.document_type == 3:
                tipo_admin.append(r.get_administration_type_display()) 
            else:
                tipo_admin.append('')
            nuevaX = '' 
            if origen == 0:   
                if r in polizasNewAdd1:
                    nuevaX = 'Nueva'  
                if r in polizasNewAdd2:
                    nuevaX = 'Renovación'   
            if r.address:
               pc = r.address.postal_code
            else:
               pc = ''
            antig = get_antiguedad(r.start_of_validity)
            if r.owner:
                val_owner = r.owner.first_name + ' '+ str(r.owner.last_name)
            else:
                val_owner = ''
            if r.paquete:
                pac = r.paquete.package_name
            else:
                pac = ''
            if r.responsable:
                val_resp = r.responsable.first_name + ' '+ str(r.responsable.last_name)
            else:
                val_resp = ''
            if r.collection_executive:
                val_col = r.collection_executive.first_name + ' '+ str(r.collection_executive.last_name)
            else:
                val_col = ''
            if r.clave:
                try:
                    cve = str(r.clave.clave)
                    cve = ((((((cve).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                except Exception as ere:
                    cve = ''
                try:
                    cve_name = str(r.clave.name)
                    cve_name = ((((((cve_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                except Exception as ere:
                    cve_name = ''
            else:
                cve = ''
                cve_name = ''
            # if not r.document_type == 11:
            if r.business_line:
                if int(r.business_line) ==1:
                    businessLine_ = 'Comercial'
                elif int(r.business_line) ==2:
                    businessLine_ = 'Personal'
                elif int(r.business_line) ==0:
                    businessLine_ = 'Otro'
                else:
                    try:
                        if int(r.business_line) ==0:
                            businessLine_ = 'Otro'
                        else:
                            businessLine_ = ''
                    except:
                        businessLine_ = ''
            else:
                businessLine_ = ''
            contratan = ''
            if r.contractor:
                contratan = ((((((r.contractor.full_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                contratanE = r.contractor.email
                contratanP = r.contractor.phone_number
                # contratanG = r.contractor.group.group_name
                if r.contractor.classification:
                    clasifica_ = r.contractor.classification.classification_name  
                else:
                    clasifica_='-----'
                if r.celula:
                    cel = r.celula.celula_name  
                else:
                    cel='-----'
                contratanG = ((((((r.contractor.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                try:
                    if r.contractor.group.type_group == 1:
                        contratanG = ((((((r.contractor.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo1_ = ''
                        grupo2_ = ''
                    elif r.contractor.group.type_group == 2:
                        grupotype1 = Group.objects.get(pk = r.contractor.group.parent.id)
                        contratanG = ((((((grupotype1.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo1_ = ((((((r.contractor.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo2_ = ''
                    elif r.contractor.group.type_group == 3:
                        grupotype1 = Group.objects.get(pk = r.contractor.group.parent.parent.id)
                        contratanG = ((((((grupotype1.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        subgrupotype2 = Group.objects.get(pk = r.contractor.group.parent.id)
                        grupo1_ = ((((((subgrupotype2.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo2_ = ((((((r.contractor.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                except Exception as e:
                    contratanG = ''
                    grupo1_ = ''
                    grupo2_ = ''
                try:
                    if r.groupinglevel:
                        if r.groupinglevel.type_grouping == 1:
                            nivelAg_ = ((((((r.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                            grupo3_ = ''
                            grupo4_ = ''
                        elif r.groupinglevel.type_grouping == 2:
                            grupotype1 = GroupingLevel.objects.get(pk = r.groupinglevel.parent.id)
                            nivelAg_ = ((((((grupotype1.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                            grupo3_ = ((((((r.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                            grupo4_ = ''
                        elif r.groupinglevel.type_grouping == 3:
                            grupotype1 = GroupingLevel.objects.get(pk = r.groupinglevel.parent.parent.id)
                            nivelAg_ = ((((((grupotype1.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                            subgrupotype2 = GroupingLevel.objects.get(pk = r.groupinglevel.parent.id)
                            grupo3_ = ((((((subgrupotype2.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                            grupo4_ = ((((((r.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    else:
                        nivelAg_ = ''
                        grupo3_ = ''
                        grupo4_ = ''  
                except Exception as e:
                    nivelAg_ = ''
                    grupo3_ = ''
                    grupo4_ = ''

                contacto = ContactInfo.objects.filter(contractor=r.contractor)

                if contacto:
                    contacto_nombre = contacto[0].name
                    contacto_telefono = contacto[0].phone_number
                    contacto_email = contacto[0].email
                else:
                    contacto_nombre = ''
                    contacto_telefono = ''
                    contacto_email = ''

            else:
                contratan = ''
                contratanE = ''
                contratanP = ''
                clasifica_='-----'                
                cel='-----'
                contratanG = ''                
                grupo1_ = ''
                grupo2_ = ''
                nivelAg_ = ''
                grupo3_ = ''
                grupo4_ = '' 
                contacto_nombre = ''
                contacto_telefono = ''
                contacto_email = ''

            try:
                old = OldPolicies.objects.filter(base_policy__id = r.id)
                try:
                  date_renovacion = old[0].created_at.strftime("%d/%m/%Y")
                except Exception as rr:
                    date_renovacion = 'Por renovar'
                try:
                    ot_renovacion = old[0].new_policy.poliza_number if old[0].new_policy.poliza_number else old[0].new_policy.internal_number
                except Exception as rr:
                    ot_renovacion = 'Por renovar'
            except Exception as dwe:
                date_renovacion = 'Por renovar'
                ot_renovacion = 'Por renovar'
            try:
                refs_policy = ReferenciadoresInvolved.objects.filter(policy = r.id,is_changed=False).order_by('referenciador__first_name').values_list('referenciador__first_name','referenciador__last_name')
                if len(refs_policy):
                    v = []
                    try:
                        for it in refs_policy:
                            v.append(it[0] +' '+ str(it[1])+str('; '))
                        referenc = v
                    except Exception as y:
                        referenc = []
                else:
                    referenc = []
            except Exception as e:
                referenc = []

            car_serial = ''
            tipoporsubramo = ''
            if r.ramo.ramo_code == 1:
                value=''
                form = Life.objects.filter(policy = r.id)
                if form:
                    try:
                        if form[0].personal:
                            value = form[0].personal.full_name
                            if form[0].personal and form[0].personal.policy_type:
                                tipoporsubramo=life_type(self,form[0].personal.policy_type)
                        else:
                            queryset = Personal_Information.objects.filter(policy = r,org_name=r.org_name)
                            if queryset:
                                if queryset[0].policy_type:
                                    tipoporsubramo=life_type(self,queryset[0].policy_type)
                                for q in queryset:
                                    if not tipoporsubramo and q.policy_type:
                                        tipoporsubramo=life_type(self,q.policy_type)
                                    if value:
                                        value = value+','+str(q.full_name)
                                    else:
                                        value = str(q.full_name)
                            else:
                                value = ''
                    except Exception as e:
                        print('error vida asegurador',e)
                        value = ''
                    valueM = '' 
                    valueMod = '' 
                    valueV = '' 
                    valueY = ''
                    valueLP = '' 
                    valueE = '' 
                    valueAd = '' 
                    valueDr = ''
                    valuePr = ''
                else:
                    value = ''
                    valueM = '' 
                    valueMod = '' 
                    valueV = '' 
                    valueY = ''
                    valueLP = '' 
                    valueE = '' 
                    valueAd = '' 
                    valueDr = ''
                    valuePr = ''
            elif r.ramo.ramo_code == 2:
                form = AccidentsDiseases.objects.filter(policy = r.id)
                if form:
                    value = form[0].personal.full_name
                    if form[0] and form[0].personal and form[0].personal.policy_type:
                        tipoporsubramo=accidentes_type(self,form[0].personal.policy_type)
                    valueM = '' 
                    valueMod = '' 
                    valueV = '' 
                    valueY = ''
                    valueLP = '' 
                    valueE = '' 
                    valueAd = '' 
                    valueDr = ''
                    valuePr = ''
                else:
                    value = ''
                    valueM = '' 
                    valueMod = '' 
                    valueV = '' 
                    valueY = ''
                    valueLP = '' 
                    valueE = '' 
                    valueAd = '' 
                    valueDr = ''
                    valuePr = ''
            elif r.subramo.subramo_code == 9:
                form = AutomobilesDamages.objects.filter(policy = r.id)
                if form:
                    tipoporsubramo=form[0].get_policy_type_display()
                    try:
                        form[0].model = ((((((form[0].model).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        form[0].version = ((((((form[0].version).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    except Exception as ers:
                        form[0].model = form[0].model
                        form[0].version = form[0].version

                    value = str(form[0].brand) + '-' + str(form[0].model )+ '-' + str(form[0].version)
                    valueM = form[0].brand 
                    valueMod = form[0].model 
                    valueV = form[0].version 
                    valueY = form[0].year 
                    valueLP = form[0].license_plates 
                    valueE = form[0].engine 
                    valueAd = form[0].adjustment 
                    valueDr = form[0].driver
                    valuePr = form[0].get_procedencia_display() if form[0] and form[0].procedencia else 'Sin especificar'

                    car_serial = form[0].serial 
                else:
                    value = ''
                    valueM = '' 
                    valueMod = '' 
                    valueV = '' 
                    valueY = ''
                    valueLP = '' 
                    valueE = '' 
                    valueAd = '' 
                    valueDr = ''
                    valuePr = ''
            elif r.ramo.ramo_code == 3 and not r.subramo.subramo_code == 9:
                form = Damages.objects.filter(policy = r.id)
                if form:
                    tipoporsubramo=form[0].get_damage_type_display()
                    value = form[0].insured_item
                    valueM = '' 
                    valueMod = '' 
                    valueV = '' 
                    valueY = ''
                    valueLP = '' 
                    valueE = '' 
                    valueAd = '' 
                    valueDr = ''
                    valuePr = ''
                else:
                    value = ''
                    valueM = '' 
                    valueMod = '' 
                    valueV = '' 
                    valueY = ''
                    valueLP = '' 
                    valueE = '' 
                    valueAd = '' 
                    valueDr = ''
                    valuePr = ''
            if value:            
                value = ((((((value).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')

            else:
                value = value
            
            if origen == 0:
                nueva.append(nuevaX)
            elif origen == 1:
                nueva.append('Nueva')
            elif origen == 2:
                nueva.append('Renovación')
            else:
                nueva.append('')
            if r.document_type ==12:
                try:
                    if r.parent.celula:
                        cel = r.parent.celula.celula_name
                    else:
                        cel = '-'
                    if r.parent.groupinglevel:
                        if r.parent.groupinglevel.type_grouping == 1:
                            nivelAg_ = ((((((r.parent.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                            grupo3_ = ''
                            grupo4_ = ''
                        elif r.parent.groupinglevel.type_grouping == 2:
                            grupotype1 = GroupingLevel.objects.get(pk = r.parent.groupinglevel.parent.id)
                            nivelAg_ = ((((((grupotype1.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                            grupo3_ = ((((((r.parent.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                            grupo4_ = ''
                        elif r.parent.groupinglevel.type_grouping == 3:
                            grupotype1 = GroupingLevel.objects.get(pk = r.parent.groupinglevel.parent.parent.id)
                            nivelAg_ = ((((((grupotype1.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                            subgrupotype2 = GroupingLevel.objects.get(pk = r.parent.groupinglevel.parent.id)
                            grupo3_ = ((((((subgrupotype2.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                            grupo4_ = ((((((r.parent.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    else:
                        nivelAg_ = ''
                        grupo3_ = ''
                        grupo4_ = ''  
                except Exception as e:
                    nivelAg_ = ''
                    grupo3_ = ''
                    grupo4_ = ''
            else:
                if r.celula:
                    cel = r.celula.celula_name
                else:
                    cel = '-'

            asegurado.append(value)
            if r.document_type ==3 or r.document_type ==11:
                completaPGC.append(completa)
            else:
                completaPGC.append('')

            inc = r.start_of_validity
            fn =r.end_of_validity
            try:
                try:
                    formatted = fn.strftime('%H/%M/%S')
                    # if formatted =='00/00/00' or formatted =='18/00/00' or formatted =='05/00/00':
                    end_date =  (str(fn).split('.')[0])
                    end_date = fn+ timedelta(hours=6, minutes=00)
                    fn = end_date
                    fn = fn
                except Exception as eft:
                    fn =r.end_of_validity
            except:
                fn = ''

            try:
                try:
                    formatted = inc.strftime('%H/%M/%S')
                    # if formatted =='00/00/00' or formatted =='18/00/00' or formatted =='05/00/00':
                    end_date =  (str(inc).split('.')[0])
                    end_date = inc+ timedelta(hours=6, minutes=00)
                    inc = end_date
                    inc = inc
                except Exception as eft:
                    inc = r.start_of_validity
            except:
                inc = ''
            statuspoliza.append(status_str)
            inicio.append(inc)
            fin.append(fn)   
            dateRen.append(date_renovacion)
            otRen.append(ot_renovacion)
            referenciador.append(referenc)
            antiguedad.append(antig)
            creadopor.append(val_owner)
            contratante.append(contratan)
            contratanteE.append(contratanE)
            contratanteP.append(contratanP)
            contratanteG.append(contratanG)
            responsable.append(val_resp)
            collection.append(val_col)
            paquete.append(pac)
            clave.append(cve)
            clave_nombre.append(cve_name)
            cp.append(pc)
            valueMA.append(valueM) 
            valueModA.append(valueMod)  
            valueVA.append(valueV) 
            valueYA.append(valueY) 
            valueLPA.append(valueLP)  
            valueEA.append(valueE)  
            valueAdA.append(valueAd)  
            valueDrA.append(valueDr) 
            valueProcedencia.append(valuePr) 
            # ---
            nivelAg.append(nivelAg_)
            grupo3.append(grupo3_)
            grupo4.append(grupo4_)
            clasifica.append(clasifica_)
            celulaC.append(cel)
            businessLine.append(businessLine_)

            contactos_nombre.append(contacto_nombre)
            contactos_email.append(contacto_email)
            contactos_telefono.append(contacto_telefono)
            cars_serials.append(car_serial)
            grupo1.append(grupo1_)
            grupo2.append(grupo2_)            
            fechac.append(fec)            
            adjuntos.append(adj)            
            comisionPercent.append(comp)
            comisionDerecho.append(comderecho)
            comisionRPF.append(comrpf)
            edocir.append(valstate)     
            contributoria.append(contrib)     
            tipo_por_subramo.append(tipoporsubramo)     
        df = pd.DataFrame()
        if 'Tipo' in columns:
            df['Tipo'] =[checkDocumentType(x) for x in polizas.values_list('document_type', flat = True)]
        if 'No.Póliza' in columns:
            df['No.Póliza'] = polizas.values_list('poliza_number', flat = True)
        if 'No.Interno' in columns:#*
            df['No.Interno'] = polizas.values_list('internal_number', flat = True)
        if 'Contratante' in columns:
            df['Contratante'] =contratante
        if 'Email' in columns:
            df['Email']=contratanteE
        if 'Grupo' in columns:
            df['Grupo'] =  contratanteG
        if 'Proveedor' in columns:
            df['Proveedor'] =polizas.values_list('aseguradora__alias', flat = True)
        if 'Ramo' in columns:
            df['Ramo']=polizas.values_list('ramo__ramo_name', flat = True)
        if 'Subramo' in columns:
            df['Subramo'] =polizas.values_list('subramo__subramo_name', flat = True)
        if 'Tipo(por subramo)' in columns:
            df['Tipo (por Subramo)'] =tipo_por_subramo
        if 'Identificador' in columns:
            df['Identificador'] = [(((((((x).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')) if x else '' for x in polizas.values_list('identifier', flat = True)]
        if 'Paquete' in columns:
            df['Paquete'] = paquete
        if 'Prima Neta' in columns:
            df['Prima Neta'] = [ float(x.p_neta) if x.p_neta else 0 for x in polizas]
        if 'Descuento' in columns:
            df['Descuento'] = [ float(x.descuento) if x.descuento else 0 for x in polizas]
        if 'RPF' in columns:
            df['RPF']= [ float(x.rpf) if x.rpf else 0 for x in polizas]
        if 'Derecho' in columns:
            df['Derecho'] =[ float(x.derecho) if x.derecho else 0 for x in polizas]
        if 'IVA' in columns:
            df['IVA'] = [ float(x.iva) if x.iva else 0 for x in polizas]
        if 'Prima Total' in columns:
            df['Prima Total'] = [ float(x.p_total) if x.p_total else 0 for x in polizas]
        if 'Comisión' in columns:
            df['Comisión'] = [ float(x.comision) if x.comision else 0 for x in polizas]
        if 'Comisión Porcentaje' in columns:
            df['Comisión Porcentaje'] = comisionPercent
        # if '%Comisión Prima Neta' in columns:
        #     df['%Comisión Prima Neta'] = comisionPercent
        # if '%Comisión Derecho' in columns:
        #     df['%Comisión Derecho'] = comisionDerecho
        # if '%Comisión RPF' in columns:
        #     df['%Comisión RPF'] = comisionRPF
        if 'Forma de Pago' in columns:
            df['Forma de Pago'] =[checkPayForm(x) for x in polizas.values_list('forma_de_pago', flat = True)]
        if 'Asegurado/Serial' in columns:
            df['Asegurado'] = asegurado
        if 'Estatus' in columns:
            df['Estatus'] =statuspoliza
        if'Inicio Póliza' in columns:
            df['Inicio Póliza'] = inicio
            try:
                df['Inicio Póliza'] = pd.to_datetime(df['Inicio Póliza'], errors='ignore', format='%D/%B/%Y').dt.date
            except Exception as e:
                df['Inicio Póliza'] = polizas.values_list('start_of_validity', flat = True) 
        if 'Fin Póliza' in columns:
            df['Fin Póliza'] = fin
            try:
                df['Fin Póliza'] = pd.to_datetime(df['Fin Póliza'], errors='ignore', format='%D/%B/%Y').dt.date
            except Exception as e:
                df['Fin Póliza'] = polizas.values_list('end_of_validity', flat = True)
        if verReferenciadores =='True' or verReferenciadores==True:
            if 'Referenciador' in columns:
                df['Referenciador'] =[str(x).replace('[','').replace(']','').replace('\'','').replace(';','') for x in referenciador]
        if 'Moneda' in columns:
            df['Moneda'] = [ checkCurrency(x) for x in polizas.values_list('f_currency', flat = True)]
        if 'Fecha creación' in columns:
            df['Fecha creación'] = polizas.values_list('created_at', flat = True) 
            try:
                df['Fecha creación'] = pd.to_datetime(df['Fecha creación'], errors='ignore', format='%D/%B/%Y').dt.date
            except Exception as e:
                df['Fecha creación'] = polizas.values_list('created_at', flat = True) 
        if 'Observaciones' in columns:
            df['Observaciones'] = [(((((((x).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')) if x else '' for x in polizas.values_list('observations', flat = True)]
        if 'Creado por' in columns:
            df['Creado por'] = creadopor
        if 'NOMBRE DEL AGENTE' in columns:
            df['NOMBRE DEL AGENTE'] = clave_nombre
        if 'CLAVE DEL AGENTE' in columns:
            df['CLAVE DEL AGENTE'] = clave
        if 'Marca' in columns:
            df['Marca'] = valueMA
        if 'Modelo' in columns:
            df['Modelo'] = valueModA
        if 'Año' in columns:
            df['Año'] =  valueYA
        if 'Versión' in columns:
            df['Versión'] = valueVA
        if 'Placas' in columns:
            df['Placas'] = valueLPA
        if 'Serie' in columns:
            df['Serie'] = cars_serials
        if 'Motor' in columns:
            df['Motor'] =valueEA
        if 'Procedencia' in columns:
            df['Procedencia'] =valueProcedencia
        if 'Adaptaciones' in columns:
            df['Adaptaciones'] = valueAdA
        if 'Dirección' in columns:
            df['Dirección'] =polizas.values_list('address__administrative_area_level_1', flat = True)
        if 'Ejecutivo Cobranza' in columns:
            df['Ejecutivo Cobranza'] = collection
        if 'Responsable' in columns:
            df['Responsable'] =responsable
        if 'Sucursal' in columns:
            df['Sucursal'] = polizas.values_list('sucursal__sucursal_name', flat = True)
        if 'Renovable' in columns:
            df['Renovable'] = [checkRenewable(x) for x in polizas.values_list('is_renewable', flat = True)]
        if 'OT/Póliza asociada' in columns:
            df['OT/Póliza asociada'] =otRen
        if 'Origen Póliza' in columns:
            df['Origen Póliza'] =nueva
        if 'Agrupación' in columns:
            df['Agrupación'] = nivelAg 
        if 'Subagrupación' in columns:
            df['Subagrupación'] =grupo3 
        if 'Subsubagrupación' in columns:
            df['Subsubagrupación'] =grupo4 
        if 'Clasificación' in columns:
            df['Clasificación'] = clasifica 
        if 'Línea de Negocio' in columns:
            df['Línea de Negocio'] =businessLine 
        if moduleName in columns:
            df[moduleName] =celulaC 
        if 'Contacto(nombre)' in columns:
            df['Contacto(nombre)'] = contactos_nombre
        if 'Contacto(telefono)' in columns:
            df['Contacto(telefono)'] =contactos_telefono
        if 'Contacto(email)' in columns:
            df['Contacto(email)'] =contactos_email
        if 'Carátula' in columns:
            df['Carátula']= caratulas
        if 'Póliza Anterior' in columns:
            df['Póliza Anterior'] = poliza_anterior
        if 'Póliza Siguiente' in columns:
            df['Póliza Siguiente'] = poliza_siguiente
        if 'Fecha Cancelación' in columns:
            df['Fecha Cancelación'] = fechac
        if 'Adjuntos' in columns:
            df['Adjuntos'] = adjuntos
        if 'Conducto de Pago' in columns:
            df['Conducto de Pago'] = conducto_de_pago
        if 'Colectividad/PG completa' in columns:
            df['Colectividad/PG completa'] = completaPGC
        if 'Estado Circulación' in columns:
            df['Estado Circulación'] = edocir
        if 'Tipo Administración' in columns:
            df['Tipo de Administración'] = tipo_admin
        if 'Bitacora' in columns:
            df['Bitacora']= bitacora
        if 'Contributoria' in columns:
            df['Contributoria']= contributoria
        if 'Seguimiento' in columns:
            df['Seguimiento'] =seguimiento
        if 'RFC de Cliente' in columns:
            df['RFC de Cliente']= rfccliente
        df['ID_SAAM'] = polizas.values_list('id', flat = True)
        
        # reporte polizas enproceso
        if add_renovadas ==1 or add_renovadas=='1':
            tipo_reporte2 = "Reporte de pólizas en proceso de renovación del " + str(since.strftime("%d/%m/%y")) + " al " + str(until.strftime("%d/%m/%y"))
            asegurado2 = []
            antiguedad2 = []
            referenciador2 = []
            creadopor2 = []
            dateRen2 = []
            otRen2 = []
            responsable2 = []
            contratante2 = []
            contratanteE2 = []
            contratanteP2 = []
            contratanteG2 = []
            collection2 = []
            clave2 = []
            clave_nombre2 = []
            cp2 = []
            caratulas2 = []
            paquete2 = []
            valueMA2 = []  
            valueModA2 = [] 
            valueVA2 = [] 
            valueYA2 = [] 
            valueLPA2 = [] 
            valueEA2 = [] 
            valueAdA2 = [] 
            valueDrA2 = []
            valueProcedencia2 = []
            tipo_admin2 = []
            nueva2 = []
            nivelAg2 = []
            grupo32 = []
            grupo42 = []
            clasifica2 = []
            poliza_siguiente2 = []
            poliza_anterior2 = []
            businessLine2 = []
            conducto_de_pago2 = []

            contactos_nombre2 = []
            contactos_email2 = []
            contactos_telefono2 = []
            celulaC2 = []
            cars_serials2 = []
            grupo12 = []       
            grupo22 = []
            fechac2 = []     
            adjuntos2 = []  
            comisionPercent2 = []  
            comisionDerecho2 = []  
            comisionRPF2 = []  
            completaPGC2 = []          
            edocir2 = []
            descuento2 = []
            correo_contratante2 = []
            ids2 = []
            polizas_enproceso = polizas_enproceso.order_by('id')
            bitacora2 = []
            contributoria2=[]
            seguimiento2=[]
            inicio2 = [] 
            fin2 = [] 
            statuspoliza2 = []
            tipo_por_subramo2 = []
            rfccliente2=[]
            # polizas = polizas.order_by('natural__full_name','juridical__j_name')
            for r in polizas_enproceso: 
                valuePr = ''
                tab = ''
                if r.document_type in [1,3,12]:
                    if r.status ==1:
                        tablero =PromotoriaTablero.objects.filter(org_name=r.org_name)#config
                        if tablero:                        
                            confTab = tablero[0].polizas_ots
                            try:
                                confTab = json.loads(confTab)
                            except Exception as eee:
                                confTab = confTab
                                try:
                                    confTab = eval(confTab)
                                except Exception as e:
                                    pass
                            for ind,y in enumerate(confTab):
                                if r.id in y['polizas']:
                                    tab=y['tablero']
                                    break
                        else:
                            tab= ''
                    else:
                        tab= ''
                else:
                    tab= ''
                seguimiento2.append(tab)
                contrib = 'No Aplica'
                if r.conducto_de_pago:
                    conducto_de_pago2.append(r.get_conducto_de_pago_display())
                else:
                    conducto_de_pago2.append('')

                comment = Comments.objects.filter(
                    model = 1, 
                    id_model = r.id,
                    is_child = False
                )
                if comment.exists():
                    comment = comment.last()
                    bitacora2.append(comment.content)
                else:
                    bitacora2.append('')

                if r.contractor and r.contractor.email:
                    correo_contratante2.append(r.contractor.email)
                else:
                    correo_contratante2.append('')
                if r.contractor and r.contractor.rfc:
                    rfccliente2.append(r.contractor.rfc)
                else:
                    rfccliente2.append('')
                ids2.append(r.id)
                completa = ''
                comp = r.comision_percent
                comderecho = r.comision_derecho_percent
                comrpf = r.comision_rpf_percent
                adj = 'No'
                if PolizasFile.objects.filter(owner__id = r.id, org_name= r.org_name).exists():
                    adj = 'Si'
                else:
                    adj  ='No'

                if r.state_circulation:
                    valstate = r.state_circulation
                    try:
                        valstate = getStates(int(r.state_circulation))
                    except:
                        valstate = r.state_circulation
                else:
                    valstate = ''
                anterior = OldPolicies.objects.filter(new_policy = r)
                posterior = OldPolicies.objects.filter(base_policy = r)
                status_str = checkStatusPolicy(r.status)
                try:
                    posterior_ots = posterior.filter(new_policy__status = 1)
                    if r.renewed_status ==2 and posterior_ots:
                        status_str = status_str+'- En proceso de renovación'
                    elif r.renewed_status ==2 and r.org_name =='ancora' and not posterior_ots:
                        status_str = status_str+'- En proceso de renovación'
                    else:
                        status_str = status_str

                except Exception as dfs:
                    print('error',dfs)
                fec = ''
                if r.status ==11:
                    if r.date_cancel:
                        try:
                            fec = r.date_cancel
                            try:
                                formatted = r.date_cancel.strftime('%H/%M/%S')
                                # if formatted =='00/00/00' or formatted =='18/00/00' or formatted =='05/00/00':
                                end_date = fec+ timedelta(hours=6, minutes=00)
                                fec = end_date
                                fec = fec
                                fec = fec.strftime("%d/%m/%Y")
                            except Exception as eft:
                                fec = r.date_cancel
                        except:
                            fec = ''
                    else:
                        fec = ''
                else:
                    fec = ''
                if anterior.exists():
                    anterior = anterior.last()
                    poliza_anterior2.append(anterior.base_policy.poliza_number)
                else:
                    poliza_anterior2.append('')

                if posterior.exists():
                    posterior = posterior.last()
                    try:
                        poliza_siguiente2.append(posterior.new_policy.poliza_number)
                    except Exception as es:
                        poliza_siguiente2.append('')
                else:
                    poliza_siguiente2.append('')
                if r.document_type ==3 or r.document_type ==11:
                    completa = 'Si'
                    if r.document_type ==3:
                        try:
                            certs = Polizas.objects.filter(parent__parent__parent__id = r.id,document_type = 6, contributory = True,org_name =r.org_name).exclude(status = 0)
                            if certs:
                                contrib = 'Si'
                            else:
                                contrib = 'No'
                        except Exception as ee:
                            print('error contrb get certs',ee) 
                            contrib='No'

                        if (r.poliza_number ==None or r.poliza_number=='') or (r.receipts_by==1 and Recibos.objects.filter(poliza__id = r.id, receipt_type =1).exclude(status = 0).count() ==0):
                            completa = 'No'
                        if Polizas.objects.filter(parent__id = r.id, document_type =4).count() ==0:
                            completa = 'No'
                        if Polizas.objects.filter(parent__parent__id = r.id, document_type =5).count() ==0:
                            completa = 'No'
                        if Polizas.objects.filter(parent__parent__parent__id = r.id, document_type =6).count() ==0:
                            completa = 'No'
                        if Package.objects.filter(policy = r.id, type_package =2).count() ==0:
                            completa = 'No'
                        if (r.receipts_by==2 and Polizas.objects.filter(parent__id = r.id, document_type =4).exists()):
                            if Recibos.objects.filter(poliza__in = list(Polizas.objects.filter(parent__id = r.id, document_type =4)), receipt_type =1).exclude(status = 0).count() ==0:
                                completa = 'No'
                        if (r.receipts_by==3 and Polizas.objects.filter(parent__parent__parent__id = r.id, document_type =6).exists()):
                            if Recibos.objects.filter(poliza__in = list(Polizas.objects.filter(parent__parent__parent__id = r.id, document_type =6)), receipt_type =1).exclude(status = 0).count() ==0:
                                completa = 'No'
                    if r.document_type ==11:
                        if (r.poliza_number ==None or r.poliza_number==''):
                            completa = 'No'
                        if Polizas.objects.filter(parent__id = r.id, document_type =12).count() ==0:
                            completa = 'No'
                if r.document_type == 12:
                    try:
                        pn = r.parent.poliza_number
                    except:
                        pn = ''
                    caratulas2.append(pn)
                    if r.date_cancel:
                        if r.status ==11:
                            try:
                                fec = r.date_cancel.strftime("%d/%m/%Y") 
                            except:
                                fec = ''
                        else:
                            fec = ''
                    else:
                        if r.status ==11:
                            try:
                                fec = r.parent.date_cancel.strftime("%d/%m/%Y") 
                            except:
                                fec = ''
                        else:
                            fec = ''
                else:
                    caratulas2.append('')
                if r.document_type == 3:
                    tipo_admin2.append(r.get_administration_type_display()) 
                else:
                    tipo_admin2.append('')
                nuevaX = '' 
                if origen == 0:   
                    if r in polizasNewAdd1:
                        nuevaX = 'Nueva'  
                    if r in polizasNewAdd2:
                        nuevaX = 'Renovación'   
                if r.address:
                    pc = r.address.postal_code
                else:
                    pc = ''
                antig = get_antiguedad(r.start_of_validity)
                if r.owner:
                    val_owner = r.owner.first_name + ' '+ str(r.owner.last_name)
                else:
                    val_owner = ''
                if r.paquete:
                    pac = r.paquete.package_name
                else:
                    pac = ''
                if r.responsable:
                    val_resp = r.responsable.first_name + ' '+ str(r.responsable.last_name)
                else:
                    val_resp = ''
                if r.collection_executive:
                    val_col = r.collection_executive.first_name + ' '+ str(r.collection_executive.last_name)
                else:
                    val_col = ''
                if r.clave:
                    try:
                        cve = str(r.clave.clave)
                        cve = ((((((cve).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    except Exception as ere:
                        cve = ''
                    try:
                        cve_name = str(r.clave.name)
                        cve_name = ((((((cve_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    except Exception as ere:
                        cve_name = ''
                else:
                    cve = ''
                    cve_name = ''
                # if not r.document_type == 11:
                if r.business_line:
                    if int(r.business_line) ==1:
                        businessLine_ = 'Comercial'
                    elif int(r.business_line) ==2:
                        businessLine_ = 'Personal'
                    elif int(r.business_line) ==0:
                        businessLine_ = 'Otro'
                    else:
                        try:
                            if int(r.business_line) ==0:
                                businessLine_ = 'Otro'
                            else:
                                businessLine_ = ''
                        except:
                            businessLine_ = ''
                else:
                    businessLine_ = ''
                contratan = ''
                if r.contractor:
                    contratan = ((((((r.contractor.full_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    contratanE = r.contractor.email
                    contratanP = r.contractor.phone_number
                    # contratanG = r.contractor.group.group_name
                    if r.contractor.classification:
                        clasifica_ = r.contractor.classification.classification_name  
                    else:
                        clasifica_='-----'
                    if r.celula:
                        cel = r.celula.celula_name  
                    else:
                        cel='-----'
                    contratanG = ((((((r.contractor.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    try:
                        if r.contractor.group.type_group == 1:
                            contratanG = ((((((r.contractor.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                            grupo1_ = ''
                            grupo2_ = ''
                        elif r.contractor.group.type_group == 2:
                            grupotype1 = Group.objects.get(pk = r.contractor.group.parent.id)
                            contratanG = ((((((grupotype1.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                            grupo1_ = ((((((r.contractor.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                            grupo2_ = ''
                        elif r.contractor.group.type_group == 3:
                            grupotype1 = Group.objects.get(pk = r.contractor.group.parent.parent.id)
                            contratanG = ((((((grupotype1.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                            subgrupotype2 = Group.objects.get(pk = r.contractor.group.parent.id)
                            grupo1_ = ((((((subgrupotype2.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                            grupo2_ = ((((((r.contractor.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    except Exception as e:
                        contratanG = ''
                        grupo1_ = ''
                        grupo2_ = ''
                    try:
                        if r.groupinglevel:
                            if r.groupinglevel.type_grouping == 1:
                                nivelAg_ = ((((((r.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                                grupo3_ = ''
                                grupo4_ = ''
                            elif r.groupinglevel.type_grouping == 2:
                                grupotype1 = GroupingLevel.objects.get(pk = r.groupinglevel.parent.id)
                                nivelAg_ = ((((((grupotype1.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                                grupo3_ = ((((((r.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                                grupo4_ = ''
                            elif r.groupinglevel.type_grouping == 3:
                                grupotype1 = GroupingLevel.objects.get(pk = r.groupinglevel.parent.parent.id)
                                nivelAg_ = ((((((grupotype1.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                                subgrupotype2 = GroupingLevel.objects.get(pk = r.groupinglevel.parent.id)
                                grupo3_ = ((((((subgrupotype2.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                                grupo4_ = ((((((r.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        else:
                            nivelAg_ = ''
                            grupo3_ = ''
                            grupo4_ = ''  
                    except Exception as e:
                        nivelAg_ = ''
                        grupo3_ = ''
                        grupo4_ = ''

                    contacto = ContactInfo.objects.filter(contractor=r.contractor)

                    if contacto:
                        contacto_nombre = contacto[0].name
                        contacto_telefono = contacto[0].phone_number
                        contacto_email = contacto[0].email
                    else:
                        contacto_nombre = ''
                        contacto_telefono = ''
                        contacto_email = ''

                else:
                    contratan = ''
                    contratanE = ''
                    contratanP = ''
                    clasifica_='-----'                
                    cel='-----'
                    contratanG = ''                
                    grupo1_ = ''
                    grupo2_ = ''
                    nivelAg_ = ''
                    grupo3_ = ''
                    grupo4_ = '' 
                    contacto_nombre = ''
                    contacto_telefono = ''
                    contacto_email = ''

                try:
                    old = OldPolicies.objects.filter(base_policy__id = r.id)
                    try:
                        date_renovacion = old[0].created_at.strftime("%d/%m/%Y")
                    except Exception as rr:
                        date_renovacion = 'Por renovar'
                    try:
                        ot_renovacion = old[0].new_policy.poliza_number if old[0].new_policy.poliza_number else old[0].new_policy.internal_number
                    except Exception as rr:
                        ot_renovacion = 'Por renovar'
                except Exception as dwe:
                    date_renovacion = 'Por renovar'
                    ot_renovacion = 'Por renovar'
                try:
                    refs_policy = ReferenciadoresInvolved.objects.filter(policy = r.id,is_changed=False).order_by('referenciador__first_name').values_list('referenciador__first_name','referenciador__last_name')
                    if len(refs_policy):
                        v = []
                        try:
                            for it in refs_policy:
                                v2.append(it[0] +' '+ str(it[1])+str('; '))
                            referenc = v
                        except Exception as y:
                            referenc = []
                    else:
                        referenc = []
                except Exception as e:
                    referenc = []

                car_serial = ''
                tipoporsubramo = ''
                if r.ramo.ramo_code == 1:
                    value=''
                    form = Life.objects.filter(policy = r.id)
                    if form:
                        try:
                            if form[0].personal:
                                value = form[0].personal.full_name
                                if form[0].personal and form[0].personal.policy_type:
                                    tipoporsubramo=life_type(self,form[0].personal.policy_type)
                            else:
                                queryset = Personal_Information.objects.filter(policy = r,org_name=r.org_name)
                                if queryset:
                                    if queryset[0].policy_type:
                                        tipoporsubramo=life_type(self,queryset[0].policy_type)
                                    for q in queryset:
                                        if not tipoporsubramo and q.policy_type:
                                            tipoporsubramo=life_type(self,q.policy_type)
                                        if value:
                                            value = value+','+str(q.full_name)
                                        else:
                                            value = str(q.full_name)
                                else:
                                    value = ''
                        except Exception as e:
                            print('error vida asegurador',e)
                            value = ''
                        valueM = '' 
                        valueMod = '' 
                        valueV = '' 
                        valueY = ''
                        valueLP = '' 
                        valueE = '' 
                        valueAd = '' 
                        valueDr = ''
                        valuePr = ''
                    else:
                        value = ''
                        valueM = '' 
                        valueMod = '' 
                        valueV = '' 
                        valueY = ''
                        valueLP = '' 
                        valueE = '' 
                        valueAd = '' 
                        valueDr = ''
                        valuePr = ''
                elif r.ramo.ramo_code == 2:
                    form = AccidentsDiseases.objects.filter(policy = r.id)
                    if form:
                        value = form[0].personal.full_name
                        if form[0] and form[0].personal and form[0].personal.policy_type:
                            tipoporsubramo=accidentes_type(self,form[0].personal.policy_type)
                        valueM = '' 
                        valueMod = '' 
                        valueV = '' 
                        valueY = ''
                        valueLP = '' 
                        valueE = '' 
                        valueAd = '' 
                        valueDr = ''
                        valuePr = ''
                    else:
                        value = ''
                        valueM = '' 
                        valueMod = '' 
                        valueV = '' 
                        valueY = ''
                        valueLP = '' 
                        valueE = '' 
                        valueAd = '' 
                        valueDr = ''
                        valuePr = ''
                elif r.subramo.subramo_code == 9:
                    form = AutomobilesDamages.objects.filter(policy = r.id)
                    if form:
                        tipoporsubramo=form[0].get_policy_type_display()
                        try:
                            form[0].model = ((((((form[0].model).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                            form[0].version = ((((((form[0].version).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        except Exception as ers:
                            form[0].model = form[0].model
                            form[0].version = form[0].version

                        value = str(form[0].brand) + '-' + str(form[0].model )+ '-' + str(form[0].version)
                        valueM = form[0].brand 
                        valueMod = form[0].model 
                        valueV = form[0].version 
                        valueY = form[0].year 
                        valueLP = form[0].license_plates 
                        valueE = form[0].engine 
                        valueAd = form[0].adjustment 
                        valueDr = form[0].driver
                        valuePr = form[0].get_procedencia_display() if form[0] and form[0].procedencia else 'Sin especificar'

                        car_serial = form[0].serial 
                    else:
                        value = ''
                        valueM = '' 
                        valueMod = '' 
                        valueV = '' 
                        valueY = ''
                        valueLP = '' 
                        valueE = '' 
                        valueAd = '' 
                        valueDr = ''
                        valuePr = ''
                elif r.ramo.ramo_code == 3 and not r.subramo.subramo_code == 9:
                    form = Damages.objects.filter(policy = r.id)
                    if form:
                        tipoporsubramo=form[0].get_damage_type_display()
                        value = form[0].insured_item
                        valueM = '' 
                        valueMod = '' 
                        valueV = '' 
                        valueY = ''
                        valueLP = '' 
                        valueE = '' 
                        valueAd = '' 
                        valueDr = ''
                        valuePr = ''
                    else:
                        value = ''
                        valueM = '' 
                        valueMod = '' 
                        valueV = '' 
                        valueY = ''
                        valueLP = '' 
                        valueE = '' 
                        valueAd = '' 
                        valueDr = ''
                        valuePr = ''
                if value:            
                    value = ((((((value).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')

                else:
                    value = value
                
                if origen == 0:
                    nueva2.append(nuevaX)
                elif origen == 1:
                    nueva2.append('Nueva')
                elif origen == 2:
                    nueva2.append('Renovación')
                else:
                    nueva2.append('')
                if r.document_type ==12:
                    try:
                        if r.parent.celula:
                            cel = r.parent.celula.celula_name
                        else:
                            cel = '-'
                        if r.parent.groupinglevel:
                            if r.parent.groupinglevel.type_grouping == 1:
                                nivelAg_ = ((((((r.parent.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                                grupo3_ = ''
                                grupo4_ = ''
                            elif r.parent.groupinglevel.type_grouping == 2:
                                grupotype1 = GroupingLevel.objects.get(pk = r.parent.groupinglevel.parent.id)
                                nivelAg_ = ((((((grupotype1.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                                grupo3_ = ((((((r.parent.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                                grupo4_ = ''
                            elif r.parent.groupinglevel.type_grouping == 3:
                                grupotype1 = GroupingLevel.objects.get(pk = r.parent.groupinglevel.parent.parent.id)
                                nivelAg_ = ((((((grupotype1.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                                subgrupotype2 = GroupingLevel.objects.get(pk = r.parent.groupinglevel.parent.id)
                                grupo3_ = ((((((subgrupotype2.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                                grupo4_ = ((((((r.parent.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        else:
                            nivelAg_ = ''
                            grupo3_ = ''
                            grupo4_ = ''  
                    except Exception as e:
                        nivelAg_ = ''
                        grupo3_ = ''
                        grupo4_ = ''
                else:
                    if r.celula:
                        cel = r.celula.celula_name
                    else:
                        cel = '-'

                asegurado2.append(value)
                if r.document_type ==3 or r.document_type ==11:
                    completaPGC2.append(completa)
                else:
                    completaPGC2.append('')

                inc = r.start_of_validity
                fn =r.end_of_validity
                try:
                    try:
                        formatted = fn.strftime('%H/%M/%S')
                        # if formatted =='00/00/00' or formatted =='18/00/00' or formatted =='05/00/00':
                        end_date =  (str(fn).split('.')[0])
                        end_date = fn+ timedelta(hours=6, minutes=00)
                        fn = end_date
                        fn = fn
                    except Exception as eft:
                        fn =r.end_of_validity
                except:
                    fn = ''

                try:
                    try:
                        formatted = inc.strftime('%H/%M/%S')
                        # if formatted =='00/00/00' or formatted =='18/00/00' or formatted =='05/00/00':
                        end_date =  (str(inc).split('.')[0])
                        end_date = inc+ timedelta(hours=6, minutes=00)
                        inc = end_date
                        inc = inc
                    except Exception as eft:
                        inc = r.start_of_validity
                except:
                    inc = ''
                statuspoliza2.append(status_str)
                inicio2.append(inc)
                fin2.append(fn)   
                dateRen2.append(date_renovacion)
                otRen2.append(ot_renovacion)
                referenciador2.append(referenc)
                antiguedad2.append(antig)
                creadopor2.append(val_owner)
                contratante2.append(contratan)
                contratanteE2.append(contratanE)
                contratanteP2.append(contratanP)
                contratanteG2.append(contratanG)
                responsable2.append(val_resp)
                collection2.append(val_col)
                paquete2.append(pac)
                clave2.append(cve)
                clave_nombre2.append(cve_name)
                cp2.append(pc)
                valueMA2.append(valueM) 
                valueModA2.append(valueMod)  
                valueVA2.append(valueV) 
                valueYA2.append(valueY) 
                valueLPA2.append(valueLP)  
                valueEA2.append(valueE)  
                valueAdA2.append(valueAd)  
                valueDrA2.append(valueDr) 
                valueProcedencia2.append(valuePr) 
                # ---
                nivelAg2.append(nivelAg_)
                grupo32.append(grupo3_)
                grupo42.append(grupo4_)
                clasifica2.append(clasifica_)
                celulaC2.append(cel)
                businessLine2.append(businessLine_)

                contactos_nombre2.append(contacto_nombre)
                contactos_email2.append(contacto_email)
                contactos_telefono2.append(contacto_telefono)
                cars_serials2.append(car_serial)
                grupo12.append(grupo1_)
                grupo22.append(grupo2_)            
                fechac2.append(fec)            
                adjuntos2.append(adj)            
                comisionPercent2.append(comp)
                comisionDerecho2.append(comderecho)
                comisionRPF2.append(comrpf)
                edocir2.append(valstate)     
                contributoria2.append(contrib)     
                tipo_por_subramo2.append(tipoporsubramo)     
            # df2 = pd.DataFrame()
            start = len(polizas) + 15
            df2 = df
            if 'Tipo' in columns:
                df2['Tipo'] =[checkDocumentType(x) for x in polizas_enproceso.values_list('document_type', flat = True)]
            # Mostrar desde la fila 30
            if 'No.Póliza' in columns:
                df2['No.Póliza'] = polizas_enproceso.values_list('poliza_number', flat = True)
            if 'No.Interno' in columns:#*
                df2['No.Interno'] = polizas_enproceso.values_list('internal_number', flat = True)
            if 'Contratante' in columns:
                df2['Contratante'] =contratante2
            if 'Email' in columns:
                df2['Email']=contratanteE2
            if 'Grupo' in columns:
                df2['Grupo'] =  contratanteG2
            if 'Proveedor' in columns:
                df2['Proveedor'] =polizas_enproceso.values_list('aseguradora__alias', flat = True)
            if 'Ramo' in columns:
                df2['Ramo']=polizas_enproceso.values_list('ramo__ramo_name', flat = True)
            if 'Subramo' in columns:
                df2['Subramo'] =polizas_enproceso.values_list('subramo__subramo_name', flat = True)
            if 'Tipo(por subramo)' in columns:
                df2['Tipo (por Subramo)'] =tipo_por_subramo2
            if 'Identificador' in columns:
                df2['Identificador'] = [(((((((x).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')) if x else '' for x in polizas_enproceso.values_list('identifier', flat = True)]
            if 'Paquete' in columns:
                df2['Paquete'] = paquete2
            if 'Prima Neta' in columns:
                df2['Prima Neta'] = [ float(x.p_neta) if x.p_neta else 0 for x in polizas_enproceso]
            if 'Descuento' in columns:
                df2['Descuento'] = [ float(x.descuento) if x.descuento else 0 for x in polizas_enproceso]
            if 'RPF' in columns:
                df2['RPF']= [ float(x.rpf) if x.rpf else 0 for x in polizas_enproceso]
            if 'Derecho' in columns:
                df2['Derecho'] =[ float(x.derecho) if x.derecho else 0 for x in polizas_enproceso]
            if 'IVA' in columns:
                df2['IVA'] = [ float(x.iva) if x.iva else 0 for x in polizas_enproceso]
            if 'Prima Total' in columns:
                df2['Prima Total'] = [ float(x.p_total) if x.p_total else 0 for x in polizas_enproceso]
            if 'Comisión' in columns:
                df2['Comisión'] = [ float(x.comision) if x.comision else 0 for x in polizas_enproceso]
            if 'Comisión Porcentaje' in columns:
                df2['Comisión Porcentaje'] = comisionPercent2
            # if '%Comisión Prima Neta' in columns:
            #     df2['%Comisión Prima Neta'] = comisionPercent
            # if '%Comisión Derecho' in columns:
            #     df2['%Comisión Derecho'] = comisionDerecho
            # if '%Comisión RPF' in columns:
            #     df2['%Comisión RPF'] = comisionRPF
            if 'Forma de Pago' in columns:
                df2['Forma de Pago'] =[checkPayForm(x) for x in polizas_enproceso.values_list('forma_de_pago', flat = True)]
            if 'Asegurado/Serial' in columns:
                df2['Asegurado'] = asegurado2
            if 'Estatus' in columns:
                df2['Estatus'] =statuspoliza2
            if'Inicio Póliza' in columns:
                df2['Inicio Póliza'] = inicio2
                try:
                    df2['Inicio Póliza'] = pd.to_datetime(df2['Inicio Póliza'], errors='ignore', format='%D/%B/%Y').dt.date
                except Exception as e:
                    df2['Inicio Póliza'] = polizas_enproceso.values_list('start_of_validity', flat = True) 
            if 'Fin Póliza' in columns:
                df2['Fin Póliza'] = fin2
                try:
                    df2['Fin Póliza'] = pd.to_datetime(df2['Fin Póliza'], errors='ignore', format='%D/%B/%Y').dt.date
                except Exception as e:
                    df2['Fin Póliza'] = polizas_enproceso.values_list('end_of_validity', flat = True)
            if verReferenciadores =='True' or verReferenciadores==True:
                if 'Referenciador' in columns:
                    df2['Referenciador'] =[str(x).replace('[','').replace(']','').replace('\'','').replace(';','') for x in referenciador2]
            if 'Moneda' in columns:
                df2['Moneda'] = [ checkCurrency(x) for x in polizas_enproceso.values_list('f_currency', flat = True)]
            if 'Fecha creación' in columns:
                df2['Fecha creación'] = polizas_enproceso.values_list('created_at', flat = True) 
                try:
                    df2['Fecha creación'] = pd.to_datetime(df2['Fecha creación'], errors='ignore', format='%D/%B/%Y').dt.date
                except Exception as e:
                    df2['Fecha creación'] = polizas_enproceso.values_list('created_at', flat = True) 
            if 'Observaciones' in columns:
                df2['Observaciones'] = [(((((((x).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')) if x else '' for x in polizas_enproceso.values_list('observations', flat = True)]
            if 'Creado por' in columns:
                df2['Creado por'] = creadopor2
            if 'NOMBRE DEL AGENTE' in columns:
                df2['NOMBRE DEL AGENTE'] = clave_nombre2
            if 'CLAVE DEL AGENTE' in columns:
                df2['CLAVE DEL AGENTE'] = clave2
            if 'Marca' in columns:
                df2['Marca'] = valueMA2
            if 'Modelo' in columns:
                df2['Modelo'] = valueModA2
            if 'Año' in columns:
                df2['Año'] =  valueYA2
            if 'Versión' in columns:
                df2['Versión'] = valueVA2
            if 'Placas' in columns:
                df2['Placas'] = valueLPA2
            if 'Serie' in columns:
                df2['Serie'] = cars_serials2
            if 'Motor' in columns:
                df2['Motor'] =valueEA2
            if 'Procedencia' in columns:
                df2['Procedencia'] =valueProcedencia2
            if 'Adaptaciones' in columns:
                df2['Adaptaciones'] = valueAdA2
            if 'Dirección' in columns:
                df2['Dirección'] =polizas_enproceso.values_list('address__administrative_area_level_1', flat = True)
            if 'Ejecutivo Cobranza' in columns:
                df2['Ejecutivo Cobranza'] = collection2
            if 'Responsable' in columns:
                df2['Responsable'] =responsable2
            if 'Sucursal' in columns:
                df2['Sucursal'] = polizas_enproceso.values_list('sucursal__sucursal_name', flat = True)
            if 'Renovable' in columns:
                df2['Renovable'] = [checkRenewable(x) for x in polizas_enproceso.values_list('is_renewable', flat = True)]
            if 'OT/Póliza asociada' in columns:
                df2['OT/Póliza asociada'] =otRen2
            if 'Origen Póliza' in columns:
                df2['Origen Póliza'] =nueva2
            if 'Agrupación' in columns:
                df2['Agrupación'] = nivelAg2
            if 'Subagrupación' in columns:
                df2['Subagrupación'] =grupo32
            if 'Subsubagrupación' in columns:
                df2['Subsubagrupación'] =grupo42 
            if 'Clasificación' in columns:
                df2['Clasificación'] = clasifica2 
            if 'Línea de Negocio' in columns:
                df2['Línea de Negocio'] =businessLine2 
            if moduleName in columns:
                df2[moduleName] =celulaC2 
            if 'Contacto(nombre)' in columns:
                df2['Contacto(nombre)'] = contactos_nombre2
            if 'Contacto(telefono)' in columns:
                df2['Contacto(telefono)'] =contactos_telefono2
            if 'Contacto(email)' in columns:
                df2['Contacto(email)'] =contactos_email2
            if 'Carátula' in columns:
                df2['Carátula']= caratulas2
            if 'Póliza Anterior' in columns:
                df2['Póliza Anterior'] = poliza_anterior2
            if 'Póliza Siguiente' in columns:
                df2['Póliza Siguiente'] = poliza_siguiente2
            if 'Fecha Cancelación' in columns:
                df2['Fecha Cancelación'] = fechac2
            if 'Adjuntos' in columns:
                df2['Adjuntos'] = adjuntos2
            if 'Conducto de Pago' in columns:
                df2['Conducto de Pago'] = conducto_de_pago2
            if 'Colectividad/PG completa' in columns:
                df2['Colectividad/PG completa'] = completaPGC2
            if 'Estado Circulación' in columns:
                df2['Estado Circulación'] = edocir2
            if 'Tipo Administración' in columns:
                df2['Tipo de Administración'] = tipo_admin2
            if 'Bitacora' in columns:
                df2['Bitacora']= bitacora2
            if 'Contributoria' in columns:
                df2['Contributoria']= contributoria2
            if 'Seguimiento' in columns:
                df2['Seguimiento'] =seguimiento2
            if 'RFC de Cliente' in columns:
                df2['RFC de Cliente']= rfccliente2
            df2['ID_SAAM'] = polizas_enproceso.values_list('id', flat = True)
        # polizas enproceso


        filename = 'reporte_polizas_%s_%s_%s-%s.xlsx'%(org, self.request.id , str(since).replace('-','_').split('T')[0], str(until).replace('-','_').split('T')[0])
        writer = pd.ExcelWriter(filename, engine='xlsxwriter')
        # Convert the dataframe to an XlsxWriter Excel object.
        df.to_excel(writer, sheet_name='Sheet1', index=False)
        df2.to_excel(writer, sheet_name='Sheet', index=False)
        # Close the Pandas Excel writer and output the Excel file.
        writer.save()
        url_response = 'timeout'
        with open(filename) as f:
            url_response = upload_to_s3(filename, org)
            try:                
                try:
                    user_ownern = post.get('user_req')
                    user_owner = User.objects.get(username = user_ownern)
                except Exception as uname:
                    user_ownern = 'superuser_'+org
                    user_owner = User.objects.get(username = user_ownern)
                # crear lanotificación
                payload = {
                    "seen": False,
                    "id_reference":0,
                    "assigned":user_ownern,
                    "org_name":org,
                    "owner":user_ownern,
                    "description":url_response,
                    "model":26,
                    "title":'El reporte (pólizas) solicitado ha sido generado'
                }
                r = requests.post(settings.API_URL + 'crear-notificacion-reportes/', (payload), verify=False)
                # notificacion, crear = Notifications.objects.get_or_create(seen=False,id_reference=0,assigned = user_owner,org_name=org,owner = user_owner,description=url_response,model=26,title='El reporte (pólizas) solicitado ha sido generado (rs2)')
            except Exception as e:
                print('Error 333', str(e))
                pass
            print(url_response)
            redis_client.publish(self.request.id, url_response)  
        # from core.utils import send_mail
        # if email:
        #     message = '''
        #     Estimado usuario, le enviamos el reporte de reclamaciones solicitado anteriormente, saludos.
        #     '''
        #     send_mail(email, filename, message)
        # else:
        #     import os
        try:
            gettingby = post['gettingby']
            print('gettingby',gettingby)
            if gettingby:
                print('reporte ancora----',gettingby)
                os.remove(filename)         
                data = {}
                data['link_reporte'] = url_response
                data['count'] = len(polizas)
                print('data-----------',data)
                return data
            else:
                os.remove(filename)
                return 'ReportePolizas Excel Executed'
        except Exception as ers:
            print('err',ers)
            os.remove(filename)
            return 'ReportePolizas Excel Executed'

redis_client = redis.Redis(host='localhost', port=6379, db=0)
@app.task(bind=True, queue=settings.QUEUE)
def reporte_polizascontrib_asincrono(self,post,otropost):
    org = post['org']
    # --------Parámetros----
    report_by = post['report_by']   
    since = post['since']
    until = post['until']
    contratante = post['contratante']
    group = post['grupo']
    type_person = post['type_contractor']
    ot_rep =  3
    order = int(post['order'])
    asc = int(post['asc'])

    excel_type = int(post['excel_type'] if 'excel_type' in post else 2)
    valDolar = 0.00
    valUdi = 0.00
    
    # Filtro de forma de pago
    fp = [12,24,6,5,4,3,2,1,7,14,15]
    st = [1,2,4,10,11,12,13,14,15]
    # Filtro de grupo
    if int(group) > 0:
        grupos1 = Group.objects.get(pk = int(group))
        subg = Group.objects.filter(parent__id = int(group), type_group = 2, org_name=post['org']).values_list('pk', flat=True)
        subsubg = Group.objects.filter(parent__id__in = subg, type_group = 3, org_name=post['org']).values_list('pk', flat=True)
        allgrupos = list(subg) + list(subsubg)
        allgrupos.append(grupos1.id)
        grupos = Group.objects.filter(pk__in = allgrupos, org_name=post['org']).values_list('pk', flat=True)
    else:
        grupos = Group.objects.filter(org_name=post['org']).values_list('pk', flat=True)


    polizas = Polizas.objects.filter(status__in = st, forma_de_pago__in = fp, org_name =post['org'],ramo__ramo_code = 2, 
        document_type = 3)
    certs = Polizas.objects.filter(parent__parent__parent__id__in = polizas.values_list('pk',flat=True),document_type = 6, contributory = True,org_name =post['org']).exclude(status = 0)
    polizas = polizas.filter(pk__in = certs.values_list('parent__parent__parent__id',flat=True))

    if int(contratante) > 0 :
        contratanten = list(Contractor.objects.filter(pk = int(contratante), group__in = grupos).values_list('pk', flat = True))
        polizas = polizas.filter(contractor__in = contratanten)
    else:
        contratanten = list(Contractor.objects.filter(group__in = grupos).values_list('pk', flat = True))
        polizas = polizas.filter(contractor__in = contratanten)

    # Filtro de fechas
    try:
        f = "%d/%m/%Y %H:%M:%S"        
        since = datetime.strptime(since , f)
        until = datetime.strptime(until , f)

    except:
        f = "%m/%d/%Y %H:%M:%S"        
        since = datetime.strptime(since , f)
        until = datetime.strptime(until , f)


    if int(report_by) == 2:
        date_filters = [Q(start_of_validity__gte=since),Q(start_of_validity__lte = until)]
    elif int(report_by) == 1:
        date_filters = [Q(created_at__gte=since),Q(created_at__lte = until), Q(migrated = False)]
    else:
        date_filters = [Q(start_of_validity__gte=since),Q(start_of_validity__lte = until)]
    polizas = polizas.filter(reduce(operator.and_, date_filters), document_type = 3, org_name = post['org'])    

    # Try
    org_name = post['org']
    origen = 0
    polizasK = polizas.values_list('pk',flat = True)
    old_policies = OldPolicies.objects.filter(org_name = org)
    polizasEvaluateN=list(old_policies.values_list('new_policy', flat=True))

    if origen == 1:
        interseccion = Polizas.objects.filter(id__in = polizasK).filter(id__in = polizasEvaluateN )
        polizas = Polizas.objects.filter(id__in = polizasK).exclude(id__in = interseccion)
    elif origen == 2:
        polizasR_ = Polizas.objects.filter(pk__in=polizasEvaluateN)
        polizasR_ = polizasR_.filter(pk__in = polizasK)
        polizas = polizasR_
    else:
        interseccion = Polizas.objects.filter(id__in = polizasK).filter(id__in = polizasEvaluateN )
        polizasNew = Polizas.objects.filter(id__in = polizasK).exclude(id__in = interseccion)
        polizasNewAdd1 = polizasNew
        # 
        polizasR_ = Polizas.objects.filter(pk__in=polizasEvaluateN)
        polizasR_ = polizasR_.filter(pk__in = polizasK)
        polizasNewAdd2 = polizasR_
    try:
        dataToFilter = getDataForPerfilRestricted(post['user'], post['org'])
    except Exception as er:
        dataToFilter = {}
    if dataToFilter:
        # Contratantes***
        polizasCl = Polizas.objects.filter(document_type__in = [1,3,11,7,8,12,6,4], org_name = post['org'])
        polizasToF = Polizas.objects.filter(document_type__in = [1,3,11,7,8], org_name = post['org'])
        if dataToFilter['ccpr']:
            polizasToF = polizasToF.filter(contractor__in = list(dataToFilter['ccpr']))
        if dataToFilter['cgpr']:
            polizasToF = polizasToF.filter(contractor__group__in = list(dataToFilter['cgpr']))
        if dataToFilter['ccepr']:
            polizasToF = polizasToF.filter(contractor__cellule__in = list(dataToFilter['ccepr']))
        if dataToFilter['crpr']:
            polizasToF = polizasToF.filter(contractor__vendor__in = list(dataToFilter['crpr']))
        if dataToFilter['cspr']:
            polizasToF = polizasToF.filter(contractor__sucursal__in = list(dataToFilter['cspr']))
        # Pólizas ****
        if dataToFilter['pppr']:
            polizasToF = polizasToF.filter(pk__in = list(dataToFilter['pppr']))
        if dataToFilter['pgpr']:
            polizasToF = polizasToF.filter(contractor__group__in = list(dataToFilter['pgpr']))
        if dataToFilter['pcepr']:
            polizasToF = polizasToF.filter(celula__in = list(dataToFilter['pcepr']))
        if dataToFilter['prpr']:
            polizasToF = polizasToF.filter(ref_policy__referenciador__in = list(dataToFilter['prpr']))
        if dataToFilter['pspr']:
            polizasToF = polizasToF.filter(sucursal__in = list(dataToFilter['pspr']))
        if dataToFilter['papr']:
            polizasToF = polizasToF.filter(groupinglevel__in = list(dataToFilter['papr']))
        if dataToFilter['pcapr']:
            polizasToF = polizasToF.filter(clave__in = list(dataToFilter['pcapr']))
        if dataToFilter['psrpr']:
            polizasToF = polizasToF.filter(subramo__subramo_code__in = list(dataToFilter['psrpr']))
        if dataToFilter['paspr']:
            polizasToF = polizasToF.filter(aseguradora__in = list(dataToFilter['paspr']))
        if dataToFilter['pstpr']:
            polizasToF = polizasToF.filter(status__in = list(dataToFilter['pstpr']))
        
        polizasCT = polizasCl.filter(document_type = 12, parent__in = list(polizasToF))
        polizasGT = polizasCl.filter(document_type = 6, parent__parent__parent__in = list(polizasToF))
        polizasFin = list(polizasToF.values_list('pk', flat = True)) + list(polizasCT.values_list('pk', flat = True)) + list(polizasGT.values_list('pk', flat = True))
        polizas = polizas.filter(pk__in = list(polizasFin)) 
    # ········ Inicia Excel ········  
    # Empieza insertado de imagen
    try:
        info_org = getInfoOrg(post)
        if len(info_org['logo']) != 0:
            archivo_imagen = 'https://miurabox.s3.amazonaws.com/cas/' + info_org['logo']
        else:
            archivo_imagen = 'saam.jpg'
    except Exception as f:
        archivo_imagen = 'saam.jpg'
    tipo_reporte = "Reporte de pólizas conributrias del " + str(since.strftime("%d/%m/%y")) + " al " + str(until.strftime("%d/%m/%y"))
    row_num = 10 # A partir de aqui pinta la tabla de resultados

    # Permiso de ver comisión
    si_com = comisions(post)
    # Estilo General
    columns = ['Tipo', 'No.Póliza', 'No. Interno', 'Contratante','Grupo', 'Email', 'Teléfono', 'Proveedor', 'Subramo',
        'Forma de Pago', 'Estatus', 'Inicio Póliza', 'Fin Póliza', 'Moneda', 'Prima Neta', 'Descuento',
        'RPF', 'Derecho', 'IVA', 'Prima Total', 'Comisión','Comisión Porcentaje', 'Clave', 'Sucursal', 'Referenciador', 'Creado por',
        'Fecha creación', 'Observaciones', 'Asegurado', 'Marca', 'Modelo', 'Versión', 'Año',
        'Placas', 'Motor', 'Adaptaciones', 'Conductor','Dirección','Código Postal','Identificador','Ejecutivo Cobranza',
        'Responsable','Ramo','Cancelacion','Renovable','OT/Póliza asociada','Origen Póliza','Paquete', 'Subgrupo','Subsubgrupo',
        'Agrupación','Subagrupación','Subsubagrupación','Clasificación','Línea Negocio','Célula','Fecha Cancelación','Adjuntos',
        'Colectividad/PG completa','Conducto de Pago']
    try:
        columns =ast.literal_eval(post['cols1'])
    except Exception as e:
        print('--errr',columns,e)
        pass        
    # ******************************************
    # Empieza insertado de imagen
    columns.insert(3,'Email')
    columns.append('ID_SAAM')
    columns.append('Conducto de Pago')
    columns.append('Contributoria')
    try:
        info_org = getInfoOrg(post)
        if len(info_org['logo']) != 0:
          archivo_imagen = 'https://miurabox.s3.amazonaws.com/cas/' + info_org['logo']
        else:
          archivo_imagen = 'saam.jpg'
    except Exception as j:
        archivo_imagen = 'saam.jpg'
    tipo_reporte = "Reporte de pólizas contributorias del " + str(since.strftime("%d/%m/%y")) + " al " + str(until.strftime("%d/%m/%y"))
       
    asegurado = []
    antiguedad = []
    referenciador = []
    creadopor = []
    dateRen = []
    otRen = []
    responsable = []
    contratante = []
    contratanteE = []
    contratanteP = []
    contratanteG = []
    collection = []
    clave = []
    cp = []
    caratulas = []
    paquete = []
    valueMA = []  
    valueModA = [] 
    valueVA = [] 
    valueYA = [] 
    valueLPA = [] 
    valueEA = [] 
    valueAdA = [] 
    valueDrA = []
    tipo_admin = []
    nueva = []
    nivelAg = []
    grupo3 = []
    grupo4 = []
    clasifica = []
    poliza_siguiente = []
    poliza_anterior = []
    businessLine = []
    conducto_de_pago = []

    contactos_nombre = []
    contactos_email = []
    contactos_telefono = []
    celulaC = []
    cars_serials = []
    grupo1 = []       
    grupo2 = []
    fechac = []     
    adjuntos = []  
    comisionPercent = []  
    completaPGC = []          
    edocir = []
    descuento = []
    correo_contratante = []
    ids = []
    polizas = polizas.order_by('id')
    bitacora = []
    contribut = []
    for r in polizas: 
        contribut.append('Si')
        if r.conducto_de_pago:
            conducto_de_pago.append(r.get_conducto_de_pago_display())
        else:
            conducto_de_pago.append('')


        comment = Comments.objects.filter(
            model = 1, 
            id_model = r.id,
            is_child = False
        )
        if comment.exists():
            comment = comment.last()
            bitacora.append(comment.content)
        else:
            bitacora.append('')

        if r.contractor.email:
            correo_contratante.append(r.contractor.email)
        else:
            correo_contratante.append('')
        ids.append(r.id)
        completa = ''
        comp = r.comision_percent
        adj = 'No'
        if PolizasFile.objects.filter(owner__id = r.id, org_name= r.org_name).exists():
            adj = 'Si'
        else:
            adj  ='No'

        if r.state_circulation:
            valstate = r.state_circulation
            try:
                valstate = getStates(int(r.state_circulation))
            except:
                valstate = r.state_circulation
        else:
            valstate = ''
        anterior = OldPolicies.objects.filter(new_policy = r)
        posterior = OldPolicies.objects.filter(base_policy = r)
        fec = ''
        if r.status ==11:
          try:
            fec = r.date_cancel.strftime("%d/%m/%Y") 
          except:
            fec = ''
        else:
          fec = ''
        if anterior.exists():
          anterior = anterior.last()
          poliza_anterior.append(anterior.base_policy.poliza_number)
        else:
          poliza_anterior.append('')

        if posterior.exists():
          posterior = posterior.last()
          try:
            poliza_siguiente.append(posterior.new_policy.poliza_number)
          except Exception as es:
            poliza_siguiente.append('')
        else:
          poliza_siguiente.append('')
        if r.document_type ==3 or r.document_type ==11:
            completa = 'Si'
            if r.document_type ==3:
                if (r.poliza_number ==None or r.poliza_number=='') or (r.receipts_by==1 and Recibos.objects.filter(poliza__id = r.id, receipt_type =1).exclude(status = 0).count() ==0):
                    completa = 'No'
                if Polizas.objects.filter(parent__id = r.id, document_type =4).count() ==0:
                    completa = 'No'
                if Polizas.objects.filter(parent__parent__id = r.id, document_type =5).count() ==0:
                    completa = 'No'
                if Polizas.objects.filter(parent__parent__parent__id = r.id, document_type =6).count() ==0:
                    completa = 'No'
                if Package.objects.filter(policy = r.id, type_package =2).count() ==0:
                    completa = 'No'
                if (r.receipts_by==2 and Polizas.objects.filter(parent__id = r.id, document_type =4).exists()):
                    if Recibos.objects.filter(poliza__in = list(Polizas.objects.filter(parent__id = r.id, document_type =4)), receipt_type =1).exclude(status = 0).count() ==0:
                        completa = 'No'
                if (r.receipts_by==3 and Polizas.objects.filter(parent__parent__parent__id = r.id, document_type =6).exists()):
                    if Recibos.objects.filter(poliza__in = list(Polizas.objects.filter(parent__parent__parent__id = r.id, document_type =6)), receipt_type =1).exclude(status = 0).count() ==0:
                        completa = 'No'
            if r.document_type ==11:
                if (r.poliza_number ==None or r.poliza_number==''):
                    completa = 'No'
                if Polizas.objects.filter(parent__id = r.id, document_type =12).count() ==0:
                    completa = 'No'
        if r.document_type == 12:

            try:
                pn = r.parent.poliza_number
            except:
                pn = ''
            caratulas.append(pn)
            if r.date_cancel:
              if r.status ==11:
                try:
                  fec = r.date_cancel.strftime("%d/%m/%Y") 
                except:
                  fec = ''
              else:
                fec = ''
            else:
              if r.status ==11:
                try:
                  fec = r.parent.date_cancel.strftime("%d/%m/%Y") 
                except:
                  fec = ''
              else:
                fec = ''
        else:
            caratulas.append('')
        if r.document_type == 3:
            tipo_admin.append(r.get_administration_type_display()) 
        else:
            tipo_admin.append('')
        nuevaX = '' 
        if origen == 0:   
            if r in polizasNewAdd1:
                nuevaX = 'Nueva'  
            if r in polizasNewAdd2:
                nuevaX = 'Renovación'   
        if r.address:
           pc = r.address.postal_code
        else:
           pc = ''
        antig = get_antiguedad(r.start_of_validity)
        if r.owner:
            val_owner = r.owner.first_name + ' '+ str(r.owner.last_name)
        else:
            val_owner = ''
        if r.paquete:
            pac = r.paquete.package_name
        else:
            pac = ''
        if r.responsable:
            val_resp = r.responsable.first_name + ' '+ str(r.responsable.last_name)
        else:
            val_resp = ''
        if r.collection_executive:
            val_col = r.collection_executive.first_name + ' '+ str(r.collection_executive.last_name)
        else:
            val_col = ''
        if r.clave:
            try:
                cve = r.clave.name + ' '+ str(r.clave.clave)
                cve = ((((((cve).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
            except Exception as ere:
                cve = ''
        else:
            cve = ''
        # if not r.document_type == 11:
        if r.business_line:
            if int(r.business_line) ==1:
                businessLine_ = 'Comercial'
            elif int(r.business_line) ==2:
                businessLine_ = 'Personal'
            elif int(r.business_line) ==0:
                businessLine_ = 'Otro'
            else:
                try:
                    if int(r.business_line) ==0:
                        businessLine_ = 'Otro'
                    else:
                        businessLine_ = ''
                except:
                    businessLine_ = ''
        else:
            businessLine_ = ''
        if r.contractor:
            contratan = ((((((r.contractor.full_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
            contratanE = r.contractor.email
            contratanP = r.contractor.phone_number
            # contratanG = r.contractor.group.group_name
            if r.contractor.classification:
                clasifica_ = r.contractor.classification.classification_name  
            else:
                clasifica_='-----'
            if r.celula:
                cel = r.celula.celula_name  
            else:
                cel='-----'
            contratanG = ((((((r.contractor.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
            try:
                if r.contractor.group.type_group == 1:
                    contratanG = ((((((r.contractor.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    grupo1_ = ''
                    grupo2_ = ''
                elif r.contractor.group.type_group == 2:
                    grupotype1 = Group.objects.get(pk = r.contractor.group.parent.id)
                    contratanG = ((((((grupotype1.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    grupo1_ = ((((((r.contractor.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    grupo2_ = ''
                elif r.contractor.group.type_group == 3:
                    grupotype1 = Group.objects.get(pk = r.contractor.group.parent.parent.id)
                    contratanG = ((((((grupotype1.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    subgrupotype2 = Group.objects.get(pk = r.contractor.group.parent.id)
                    grupo1_ = ((((((subgrupotype2.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    grupo2_ = ((((((r.contractor.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
            except Exception as e:
                contratanG = ''
                grupo1_ = ''
                grupo2_ = ''
            try:
                if r.groupinglevel:
                    if r.groupinglevel.type_grouping == 1:
                        nivelAg_ = ((((((r.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo3_ = ''
                        grupo4_ = ''
                    elif r.groupinglevel.type_grouping == 2:
                        grupotype1 = GroupingLevel.objects.get(pk = r.groupinglevel.parent.id)
                        nivelAg_ = ((((((grupotype1.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo3_ = ((((((r.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo4_ = ''
                    elif r.groupinglevel.type_grouping == 3:
                        grupotype1 = GroupingLevel.objects.get(pk = r.groupinglevel.parent.parent.id)
                        nivelAg_ = ((((((grupotype1.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        subgrupotype2 = GroupingLevel.objects.get(pk = r.groupinglevel.parent.id)
                        grupo3_ = ((((((subgrupotype2.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo4_ = ((((((r.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                else:
                    nivelAg_ = ''
                    grupo3_ = ''
                    grupo4_ = ''  
            except Exception as e:
                nivelAg_ = ''
                grupo3_ = ''
                grupo4_ = ''

            contacto = ContactInfo.objects.filter(contractor=r.contractor)

            if contacto:
                contacto_nombre = contacto[0].name
                contacto_telefono = contacto[0].phone_number
                contacto_email = contacto[0].email
            else:
                contacto_nombre = ''
                contacto_telefono = ''
                contacto_email = ''

        else:
            contratan = ''
            contratanE = ''
            contratanP = ''
            clasifica_='-----'                
            cel='-----'
            contratanG = ''                
            grupo1_ = ''
            grupo2_ = ''
            nivelAg_ = ''
            grupo3_ = ''
            grupo4_ = '' 
            contacto_nombre = ''
            contacto_telefono = ''
            contacto_email = ''

        try:
            old = OldPolicies.objects.filter(base_policy__id = r.id)
            try:
              date_renovacion = old[0].created_at.strftime("%d/%m/%Y")
            except Exception as rr:
                date_renovacion = 'Por renovar'
            try:
                ot_renovacion = old[0].new_policy.poliza_number if old[0].new_policy.poliza_number else old[0].new_policy.internal_number
            except Exception as rr:
                ot_renovacion = 'Por renovar'
        except Exception as dwe:
            date_renovacion = 'Por renovar'
            ot_renovacion = 'Por renovar'
        try:
            refs_policy = ReferenciadoresInvolved.objects.filter(policy = r.id,is_changed=False).values_list('referenciador__first_name','referenciador__last_name')
            if len(refs_policy):
                v = []
                try:
                    for it in refs_policy:
                        v.append(it[0] +' '+ str(it[1])+str('; '))
                    referenc = v
                except Exception as y:
                    referenc = []
            else:
                referenc = []
        except Exception as e:
            referenc = []

        car_serial = ''
        
        if r.ramo.ramo_code == 1:
            form = Life.objects.filter(policy = r.id)
            if form:
                try:
                    if form[0].personal:
                        value = form[0].personal.full_name
                    else:
                        value = ''
                except Exception as e:
                    value = ''
                valueM = '' 
                valueMod = '' 
                valueV = '' 
                valueY = ''
                valueLP = '' 
                valueE = '' 
                valueAd = '' 
                valueDr = ''
            else:
                value = ''
                valueM = '' 
                valueMod = '' 
                valueV = '' 
                valueY = ''
                valueLP = '' 
                valueE = '' 
                valueAd = '' 
                valueDr = ''
        elif r.ramo.ramo_code == 2:
            form = AccidentsDiseases.objects.filter(policy = r.id)
            if form:
                value = form[0].personal.full_name
                valueM = '' 
                valueMod = '' 
                valueV = '' 
                valueY = ''
                valueLP = '' 
                valueE = '' 
                valueAd = '' 
                valueDr = ''
            else:
                value = ''
                valueM = '' 
                valueMod = '' 
                valueV = '' 
                valueY = ''
                valueLP = '' 
                valueE = '' 
                valueAd = '' 
                valueDr = ''
        elif r.subramo.subramo_code == 9:
            form = AutomobilesDamages.objects.filter(policy = r.id)
            if form:
                try:
                    form[0].model = ((((((form[0].model).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    form[0].version = ((((((form[0].version).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                except Exception as ers:
                    form[0].model = form[0].model
                    form[0].version = form[0].version

                value = str(form[0].brand) + '-' + str(form[0].model )+ '-' + str(form[0].version)
                valueM = form[0].brand 
                valueMod = form[0].model 
                valueV = form[0].version 
                valueY = form[0].year 
                valueLP = form[0].license_plates 
                valueE = form[0].engine 
                valueAd = form[0].adjustment 
                valueDr = form[0].driver

                car_serial = form[0].serial 
            else:
                value = ''
                valueM = '' 
                valueMod = '' 
                valueV = '' 
                valueY = ''
                valueLP = '' 
                valueE = '' 
                valueAd = '' 
                valueDr = ''
        elif r.ramo.ramo_code == 3 and not r.subramo.subramo_code == 9:
            form = Damages.objects.filter(policy = r.id)
            if form:
                value = form[0].insured_item
                valueM = '' 
                valueMod = '' 
                valueV = '' 
                valueY = ''
                valueLP = '' 
                valueE = '' 
                valueAd = '' 
                valueDr = ''
            else:
                value = ''
                valueM = '' 
                valueMod = '' 
                valueV = '' 
                valueY = ''
                valueLP = '' 
                valueE = '' 
                valueAd = '' 
                valueDr = ''
        if value:
            value = ((((((value).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'') 
        else:
            value = value
        
        if origen == 0:
            nueva.append(nuevaX)
        elif origen == 1:
            nueva.append('Nueva')
        elif origen == 2:
            nueva.append('Renovación')
        else:
            nueva.append('')
        if r.document_type ==12:
            try:
                if r.parent.celula:
                    cel = r.parent.celula.celula_name
                else:
                    cel = '-'
                if r.parent.groupinglevel:
                    if r.parent.groupinglevel.type_grouping == 1:
                        nivelAg_ = ((((((r.parent.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo3_ = ''
                        grupo4_ = ''
                    elif r.parent.groupinglevel.type_grouping == 2:
                        grupotype1 = GroupingLevel.objects.get(pk = r.parent.groupinglevel.parent.id)
                        nivelAg_ = ((((((grupotype1.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo3_ = ((((((r.parent.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo4_ = ''
                    elif r.parent.groupinglevel.type_grouping == 3:
                        grupotype1 = GroupingLevel.objects.get(pk = r.parent.groupinglevel.parent.parent.id)
                        nivelAg_ = ((((((grupotype1.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        subgrupotype2 = GroupingLevel.objects.get(pk = r.parent.groupinglevel.parent.id)
                        grupo3_ = ((((((subgrupotype2.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo4_ = ((((((r.parent.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                else:
                    nivelAg_ = ''
                    grupo3_ = ''
                    grupo4_ = ''  
            except Exception as e:
                nivelAg_ = ''
                grupo3_ = ''
                grupo4_ = ''
        else:
            if r.celula:
                cel = r.celula.celula_name
            else:
                cel = '-'
        asegurado.append(value)
        if r.document_type ==3 or r.document_type ==11:
            completaPGC.append(completa)
        else:
            completaPGC.append('')
        dateRen.append(date_renovacion)
        otRen.append(ot_renovacion)
        referenciador.append(referenc)
        antiguedad.append(antig)
        creadopor.append(val_owner)
        contratante.append(contratan)
        contratanteE.append(contratanE)
        contratanteP.append(contratanP)
        contratanteG.append(contratanG)
        responsable.append(val_resp)
        collection.append(val_col)
        paquete.append(pac)
        clave.append(cve)
        cp.append(pc)
        valueMA.append(valueM) 
        valueModA.append(valueMod)  
        valueVA.append(valueV) 
        valueYA.append(valueY) 
        valueLPA.append(valueLP)  
        valueEA.append(valueE)  
        valueAdA.append(valueAd)  
        valueDrA.append(valueDr) 
        # ---
        nivelAg.append(nivelAg_)
        grupo3.append(grupo3_)
        grupo4.append(grupo4_)
        clasifica.append(clasifica_)
        celulaC.append(cel)
        businessLine.append(businessLine_)

        contactos_nombre.append(contacto_nombre)
        contactos_email.append(contacto_email)
        contactos_telefono.append(contacto_telefono)
        cars_serials.append(car_serial)
        grupo1.append(grupo1_)
        grupo2.append(grupo2_)            
        fechac.append(fec)            
        adjuntos.append(adj)            
        comisionPercent.append(comp)
        edocir.append(valstate)     
    
    df = pd.DataFrame()
    if 'Tipo' in columns:
        df['Tipo'] =[checkDocumentType(x) for x in polizas.values_list('document_type', flat = True)]
    if 'No.Póliza' in columns:
        df['No.Póliza'] = polizas.values_list('poliza_number', flat = True)
    if 'No.Interno' in columns:#*
        df['No.Interno'] = polizas.values_list('internal_number', flat = True)
    if 'Contratante' in columns:
        df['Contratante'] =contratante
    if 'Email' in columns:
        df['Email']=contratanteE
    if 'Grupo' in columns:
        df['Grupo'] =  contratanteG
    if 'Proveedor' in columns:
        df['Proveedor'] =polizas.values_list('aseguradora__alias', flat = True)
    if 'Ramo' in columns:
        df['Ramo']=polizas.values_list('ramo__ramo_name', flat = True)
    if 'Subramo' in columns:
        df['Subramo'] =polizas.values_list('subramo__subramo_name', flat = True)
    if 'Identificador' in columns:
        df['Identificador'] = [(((((((x).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')) if x else '' for x in polizas.values_list('identifier', flat = True)]
    if 'Paquete' in columns:
        df['Paquete'] = paquete
    if 'Prima Neta' in columns:
        df['Prima Neta'] = [ float(x.p_neta) if x.p_neta else 0 for x in polizas]
    if 'Descuento' in columns:
        df['Descuento'] = list(map(float, polizas.values_list('descuento', flat = True)))
    if 'RPF' in columns:
        df['RPF']= [ float(x.rpf) if x.rpf else 0 for x in polizas]
    if 'Derecho' in columns:
        df['Derecho'] =[ float(x.derecho) if x.derecho else 0 for x in polizas]
    if 'IVA' in columns:
        df['IVA'] = [ float(x.iva) if x.iva else 0 for x in polizas]
    if 'Prima Total' in columns:
        df['Prima Total'] = [ float(x.p_total) if x.p_total else 0 for x in polizas]
    if 'Comisión' in columns:
        df['Comisión'] = [ float(x.comision) if x.comision else 0 for x in polizas]
    if 'Comisión Porcentaje' in columns:
        df['Comisión Porcentaje'] = comisionPercent
    if 'Forma de Pago' in columns:
        df['Forma de Pago'] =[checkPayForm(x) for x in polizas.values_list('forma_de_pago', flat = True)]
    if 'Asegurado/Serial' in columns:
        df['Asegurado'] = asegurado
    if 'Estatus' in columns:
        df['Estatus'] =[ checkStatusPolicy(x) for x in  polizas.values_list('status', flat = True)]
    if'Inicio Póliza' in columns:
        df['Inicio Póliza'] = polizas.values_list('start_of_validity', flat = True)
    if 'Fin Póliza' in columns:
        df['Fin Póliza'] = polizas.values_list('end_of_validity', flat = True)
    if 'Referenciador' in columns:
        df['Referenciador'] =[str(x).replace('[','').replace(']','').replace('\'','').replace(';','') for x in referenciador]
    if 'Moneda' in columns:
        df['Moneda'] = [ checkCurrency(x) for x in polizas.values_list('f_currency', flat = True)]
    if 'Fecha creación' in columns:
        df['Fecha creación'] = polizas.values_list('created_at', flat = True) 
    if 'Observaciones' in columns:
        df['Observaciones'] = [(((((((x).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')) if x else '' for x in polizas.values_list('observations', flat = True)]
    if 'Creado por' in columns:
        df['Creado por'] = creadopor
    if 'Clave' in columns:
        df['Clave'] = clave
    if 'Marca' in columns:
        df['Marca'] = valueMA
    if 'Modelo' in columns:
        df['Modelo'] = valueModA
    if 'Año' in columns:
        df['Año'] =  valueYA
    if 'Versión' in columns:
        df['Versión'] = valueVA
    if 'Placas' in columns:
        df['Placas'] = valueLPA
    if 'Serie' in columns:
        df['Serie'] = cars_serials
    if 'Motor' in columns:
        df['Motor'] =valueEA
    if 'Adaptaciones' in columns:
        df['Adaptaciones'] = valueAdA
    if 'Dirección' in columns:
        df['Dirección'] =polizas.values_list('address__administrative_area_level_1', flat = True)
    if 'Ejecutivo Cobranza' in columns:
        df['Ejecutivo Cobranza'] = collection
    if 'Responsable' in columns:
        df['Responsable'] =responsable
    if 'Sucursal' in columns:
        df['Sucursal'] = polizas.values_list('sucursal__sucursal_name', flat = True)
    if 'Renovable' in columns:
        df['Renovable'] = [checkRenewable(x) for x in polizas.values_list('is_renewable', flat = True)]
    if 'OT/Póliza asociada' in columns:
        df['OT/Póliza asociada'] =otRen
    if 'Origen Póliza' in columns:
        df['Origen Póliza'] =nueva
    if 'Agrupación' in columns:
        df['Agrupación'] = nivelAg 
    if 'Subagrupación' in columns:
        df['Subagrupación'] =grupo3 
    if 'Subsubagrupación' in columns:
        df['Subsubagrupación'] =grupo4 
    if 'Clasificación' in columns:
        df['Clasificación'] = clasifica 
    if 'Línea de Negocio' in columns:
        df['Línea de Negocio'] =businessLine 
    if 'Célula' in columns:
        df['Célula'] =celulaC 
    if 'Contacto(nombre)' in columns:
        df['Contacto(nombre)'] = contactos_nombre
    if 'Contacto(telefono)' in columns:
        df['Contacto(telefono)'] =contactos_telefono
    if 'Contacto(email)' in columns:
        df['Contacto(email)'] =contactos_email
    if 'Carátula' in columns:
        df['Carátula']= caratulas
    if 'Póliza Anterior' in columns:
        df['Póliza Anterior'] = poliza_anterior
    if 'Póliza Siguiente' in columns:
        df['Póliza Siguiente'] = poliza_siguiente
    if 'Fecha Cancelación' in columns:
        df['Fecha Cancelación'] = fechac
    if 'Adjuntos' in columns:
        df['Adjuntos'] = adjuntos
    if 'Conducto de Pago' in columns:
        df['Conducto de Pago'] = conducto_de_pago
    if 'Colectividad/PG completa' in columns:
        df['Colectividad/PG completa'] = completaPGC
    if 'Estado Circulación' in columns:
        df['Estado Circulación'] = edocir
    if 'Tipo Administración' in columns:
        df['Tipo de Administración'] = tipo_admin
    if 'Bitacora' in columns:
        df['Bitacora']= bitacora
    if 'Contributoria' in columns:
        df['Contributoria'] = contribut
    # if 'RFC Clave' in columns:
    #     df['RFC Clave'] = polizas.values_list('rfc_cve', flat = True)
    # if 'RFC HomoClave' in columns:
    #     df['RFC HomoClave'] = polizas.values_list('rfc_homocve', flat = True)
    # if 'Domicilio Calle-Núm' in columns:
    #     df['Domicilio Calle-Núm'] = polizas.values_list('dom_callenum', flat = True)
    # if 'Domicilio Colonia' in columns:
    #     df['Domicilio Colonia'] = polizas.values_list('dom_colonia', flat = True)
    # if 'Domicilio C.P.' in columns:
    #     df['Domicilio C.P.'] = polizas.values_list('dom_cp', flat = True)
    # if 'Domicilio Población/Municipio' in columns:
    #     df['Domicilio Población/Municipio'] = polizas.values_list('dom_poblacion', flat = True)
    # if 'Domicilio Estado' in columns:
    #     df['Domicilio Estado'] = polizas.values_list('dom_estado', flat = True)
    df['ID_SAAM'] = polizas.values_list('id', flat = True)
    
    try:
        df['Inicio Póliza'] = pd.to_datetime(df['Inicio Póliza'], errors='ignore', format='%D/%B/%Y').dt.date
    except Exception as e:
        df['Inicio Póliza'] = polizas.values_list('start_of_validity', flat = True) 

    try:
        df['Fin Póliza'] = pd.to_datetime(df['Fin Póliza'], errors='ignore', format='%D/%B/%Y').dt.date
    except Exception as e:
        df['Fin Póliza'] = polizas.values_list('end_of_validity', flat = True) 

    try:
        df['Fecha creación'] = pd.to_datetime(df['Fecha creación'], errors='ignore', format='%D/%B/%Y').dt.date
    except Exception as e:
        df['Fecha creación'] = polizas.values_list('created_at', flat = True) 


    # obj['columns'] = str(list(columns))
    # obj['imagen'] = archivo_imagen
    # obj['registros'] = len(polizas)

    filename = 'reporte_polizascontributorias_%s_%s_%s-%s.xlsx'%(org, self.request.id , str(since).replace('-','_').split('T')[0], str(until).replace('-','_').split('T')[0])
    writer = pd.ExcelWriter(filename, engine='xlsxwriter')
    # Convert the dataframe to an XlsxWriter Excel object.
    df.to_excel(writer, sheet_name='PólizasContributorias', index=False)
    # Close the Pandas Excel writer and output the Excel file.
    writer.save()
    url_response = 'timeout'
    with open(filename) as f:
        url_response = upload_to_s3(filename, org)
        print(url_response)
        redis_client.publish(self.request.id, url_response)  
        os.remove(filename)
    # from core.utils import send_mail
    # if email:
    #     message = '''
    #     Estimado usuario, le enviamos el reporte de reclamaciones solicitado anteriormente, saludos.
    #     '''
    #     send_mail(email, filename, message)
    # else:
    #     import os
    return 'ReportePolizas Excel Executed'

# reporte detalle de auditoria
redis_client = redis.Redis(host='localhost', port=6379, db=0)
@app.task(bind=True, queue=settings.QUEUE)
def reporte_auditoria_asincrono(self,post,otropost):
    start_time = time.perf_counter()
    org = post['org']
    # --------Parámetros----
    provider = post['providers']
    ramo = post['ramos']
    subramo = post['subramos'] 
    since = post['since']
    until = post['until']
    pre = int(post['ot'])

    grupo = post['grupo']  
    identifier = post['identifier']  
    endosos = post['ot']  

    try:
        ramos_sel = ast.literal_eval(post['ramos'])
    except:
        ramos_sel = None
    try:
        subramos_sel = ast.literal_eval(post['subramos'])
    except:
        subramos_sel =None

    try:
        providers_sel = ast.literal_eval(post['providers'])
    except:
        providers_sel = None

    fp = [12,24,6,5,4,3,2,1,7,14,15]
    st = [1,2,4,10,11,12,13,14,15]
    # Filtro de aseguradora
    if providers_sel and providers_sel != '0':
        providers = list(Provider.objects.filter(pk__in = (providers_sel)).values_list('pk', flat=True))
    else:
        providers = list(Provider.objects.filter(org_name = post['org']).values_list('pk', flat=True))
    # Filtro de ramo
    if ramos_sel and ramos_sel != '0':
        ramos = list(Ramos.objects.filter(org_name = post['org'], provider__in = providers,ramo_code__in=ramos_sel).values_list('pk', flat=True))
    else:
        ramos = list(Ramos.objects.filter(org_name = post['org'], provider__in = providers).values_list('pk', flat=True))    # Filtro de subramo
    if subramos_sel and subramos_sel != '0':
        subramos = list(SubRamos.objects.filter(org_name = post['org'], ramo__in = ramos,subramo_code__in= subramos_sel).values_list('pk', flat=True))
    else:
        subramos = list(SubRamos.objects.filter(org_name = post['org'], ramo__in = ramos).values_list('pk', flat=True))   

    polizas = Polizas.objects.filter(org_name = post['org'],aseguradora__in = providers,
        ramo__in = ramos,subramo__in = subramos).exclude(status__in = [1,2,0]).exclude(document_type__in = [2,6,10,7,8,4,5])

    # Filtro de fechas
    try:
        f = "%d/%m/%Y %H:%M:%S"        
        since = datetime.strptime(since , f)
        until = datetime.strptime(until , f)
    except:
        f = "%m/%d/%Y %H:%M:%S"        
        since = datetime.strptime(since , f)
        until = datetime.strptime(until , f)

    # Filtro fechas desde-hasta inicio polizas vigencia

    date_filters = [Q(start_of_validity__gte=since),Q(start_of_validity__lte = until)]
    polizas = polizas.filter(reduce(operator.and_, date_filters))
    if identifier:
        polizas = polizas.filter(identifier__icontains=identifier)
    if int(grupo) !=0:
        polizas = polizas.filter(contractor__group__id=grupo)
    try:
        dataToFilter = getDataForPerfilRestricted(post['user'], post['org'])
    except Exception as er:
        dataToFilter = {}
    if dataToFilter:
        # Contratantes***
        polizasCl = Polizas.objects.filter(document_type__in = [1,3,11,7,8,12,6,4], org_name = post['org'])
        polizasToF = Polizas.objects.filter(document_type__in = [1,3,11,7,8], org_name = post['org'])
        if dataToFilter['ccpr']:
            polizasToF = polizasToF.filter(contractor__in = list(dataToFilter['ccpr']))
        if dataToFilter['cgpr']:
            polizasToF = polizasToF.filter(contractor__group__in = list(dataToFilter['cgpr']))
        if dataToFilter['ccepr']:
            polizasToF = polizasToF.filter(contractor__cellule__in = list(dataToFilter['ccepr']))
        if dataToFilter['crpr']:
            polizasToF = polizasToF.filter(contractor__vendor__in = list(dataToFilter['crpr']))
        if dataToFilter['cspr']:
            polizasToF = polizasToF.filter(contractor__sucursal__in = list(dataToFilter['cspr']))
        # Pólizas ****
        if dataToFilter['pppr']:
            polizasToF = polizasToF.filter(pk__in = list(dataToFilter['pppr']))
        if dataToFilter['pgpr']:
            polizasToF = polizasToF.filter(contractor__group__in = list(dataToFilter['pgpr']))
        if dataToFilter['pcepr']:
            polizasToF = polizasToF.filter(celula__in = list(dataToFilter['pcepr']))
        if dataToFilter['prpr']:
            polizasToF = polizasToF.filter(ref_policy__referenciador__in = list(dataToFilter['prpr']))
        if dataToFilter['pspr']:
            polizasToF = polizasToF.filter(sucursal__in = list(dataToFilter['pspr']))
        if dataToFilter['papr']:
            polizasToF = polizasToF.filter(groupinglevel__in = list(dataToFilter['papr']))
        if dataToFilter['pcapr']:
            polizasToF = polizasToF.filter(clave__in = list(dataToFilter['pcapr']))
        if dataToFilter['psrpr']:
            polizasToF = polizasToF.filter(subramo__subramo_code__in = list(dataToFilter['psrpr']))
        if dataToFilter['paspr']:
            polizasToF = polizasToF.filter(aseguradora__in = list(dataToFilter['paspr']))
        if dataToFilter['pstpr']:
            polizasToF = polizasToF.filter(status__in = list(dataToFilter['pstpr']))
        
        polizasCT = polizasCl.filter(document_type = 12, parent__in = list(polizasToF))
        polizasGT = polizasCl.filter(document_type = 6, parent__parent__parent__in = list(polizasToF))
        polizasFin = list(polizasToF.values_list('pk', flat = True)) + list(polizasCT.values_list('pk', flat = True)) + list(polizasGT.values_list('pk', flat = True))
        polizas = polizas.filter(pk__in = list(polizasFin)) 
    # ········ Inicia Excel ········  
    try:        
        # Empieza insertado de imagen
        info_org = getInfoOrg(post)
        if len(info_org['logo']) != 0:
          archivo_imagen = 'https://miurabox.s3.amazonaws.com/cas/' + info_org['logo']
        else:
          archivo_imagen = 'saam.jpg'
    except Exception as er:
        print('erorrr',er)
        archivo_imagen = 'saam.jpg'
    tipo_reporte = "Detalle de Auditoría "+post['org'] +" del " + str(since.strftime("%d/%m/%y")) + " al " + str(until.strftime("%d/%m/%y"))
    row_num = 10 # A partir de aqui pinta la tabla de resultados

    try:
        columns =ast.literal_eval(post['cols1'])
    except Exception as e:
        print('--errr',columns,e)
        pass        
    # ******************************************
       
    asegurado = []
    antiguedad = []
    referenciador = []
    creadopor = []
    dateRen = []
    otRen = []
    responsable = []
    contratante = []
    contratanteE = []
    contratanteP = []
    contratanteG = []
    collection = []
    clave = []
    cp = []
    caratulas = []
    paquete = []
    valueMA = []  
    valueModA = [] 
    valueVA = [] 
    valueYA = [] 
    valueLPA = [] 
    valueEA = [] 
    valueAdA = [] 
    valueDrA = []
    tipo_admin = []
    nueva = []
    nivelAg = []
    grupo3 = []
    grupo4 = []
    clasifica = []
    poliza_siguiente = []
    poliza_anterior = []
    businessLine = []
    conducto_de_pago = []

    contactos_nombre = []
    contactos_email = []
    contactos_telefono = []
    celulaC = []
    cars_serials = []
    grupo1 = []       
    grupo2 = []
    fechac = []     
    adjuntos = []  
    comisionPercent = []  
    completaPGC = []          
    edocir = []
    descuento = []
    correo_contratante = []
    ids = []
    polizas = polizas.order_by('id')
    bitacora = []
    contributoria=[]
    seguimiento=[]
    tipo_registro=[]
    num_poliza=[]
    identifi = []
    provider=[]
    desc = []
    ramo_ = []
    subramo_ = []
    conteo_tipo = []
    numendosos=[]
    endosoconcepto=[]
    tipodato=[]
    idpoliza = []
    serierecibo = []
    estatusrecibo = []
    iniciorecibo = []
    finrecibo = []
    fechaconciliacion = []
    foliopago = []
    folioliquidacion = []
    comisionconciliada = []
    pagadopor=[]
    liquidadopor=[]
    balancepneta=[]

    balanceptotal = []
    totalpagado = []
    totalpendiente = []
    totalcancelado = []
    otstramite = []
    recibosvencidos = []
    balancepolizarecibos = []
    tiempovigenciacreacion = []
    tiemporegistropago = []
    tiempoliquidacion = []
    idrecibo = []
    internal = []
    comision = []
    comisionp = []
    pneta = []
    rpf = []
    iva = []
    derecho = []
    descuento = []
    ptotal = []
    asg = []
    rm = []
    identifier = []
    srm = []
    estado = []
    inivig = []
    endvig = []
    fpago = []
    moneda = []
    observac = []
    creada = []
    tipoasegurado=[]

    vencimiento=[] 
    pnetarecibo=[]  
    rpfrecibo = []
    derechorecibo=[]
    ivarecibo =[]
    ptotalrecibo = []
    comisionrecibo=[]
    fechapago=[]
    sttaus_poliza = []    
    verReferenciadores = post.get('verReferenciadores', True)
    # polizas = polizas.order_by('natural__full_name','juridical__j_name')
    for idx,r in enumerate(polizas):
        statusr = checkStatusPolicy(r.status)
        valR = False
        if OldPolicies.objects.filter(base_policy__id = r.id, new_policy__status = 1).exists():
            valR = True
        if r.org_name == 'ancora':
            if r.status==13 and r.is_renewable==1 or (r.renewed_status ==2 and r.status==13):
                statusr = statusr+' -En Proceso de Renovación'
        contrib = 'No Aplica'
        tasegurado = ''
        conteo_tipo.append(idx+1)
        if r.conducto_de_pago:
            conducto_de_pago.append(r.get_conducto_de_pago_display())
        else:
            conducto_de_pago.append('')
        comment = Comments.objects.filter(
            model = 1, 
            id_model = r.id,
            is_child = False
        )
        if comment.exists():
            comment = comment.last()
            bitacora.append(comment.content)
        else:
            bitacora.append('')

        if r.contractor.email:
            correo_contratante.append(r.contractor.email)
        else:
            correo_contratante.append('')
        ids.append(r.id)
        completa = ''
        comp = r.comision_percent
        adj = 'No'
        desc.append((((((((r.observations if r.observations else '').replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')))
        if PolizasFile.objects.filter(owner__id = r.id, org_name= r.org_name).exists():
            adj = 'Si'
        else:
            adj  ='No'

        anterior = OldPolicies.objects.filter(new_policy = r)
        posterior = OldPolicies.objects.filter(base_policy = r)
        fec = ''
        if r.status ==11:
          try:
            fec = r.date_cancel.strftime("%d/%m/%Y") 
          except:
            fec = ''
        else:
          fec = ''
        if anterior.exists():
          anterior = anterior.last()
          poliza_anterior.append(anterior.base_policy.poliza_number)
        else:
          poliza_anterior.append('')

        if posterior.exists():
          posterior = posterior.last()
          try:
            poliza_siguiente.append(posterior.new_policy.poliza_number)
          except Exception as es:
            poliza_siguiente.append('')
        else:
          poliza_siguiente.append('')
        provider.append(r.aseguradora.alias if r.aseguradora else '')
        ramo_.append(r.ramo.ramo_name if r.ramo else '')
        subramo_.append(r.subramo.subramo_name if r.subramo else '')
        identifi.append((((((((r.identifier if r.identifier else '').replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')))
        nuevaX = '' 

        antig = get_antiguedad(r.start_of_validity)
        if r.owner:
            val_owner = r.owner.first_name + ' '+ str(r.owner.last_name)
        else:
            val_owner = ''
        if r.paquete:
            pac = r.paquete.package_name
        else:
            pac = ''
        if r.responsable:
            val_resp = r.responsable.first_name + ' '+ str(r.responsable.last_name)
        else:
            val_resp = ''
        if r.collection_executive:
            val_col = r.collection_executive.first_name + ' '+ str(r.collection_executive.last_name)
        else:
            val_col = ''
        if r.clave:
            try:
                cve = r.clave.name + ' '+ str(r.clave.clave)
                cve = ((((((cve).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
            except Exception as ere:
                cve = ''
        else:
            cve = ''
        if r.business_line:
            if int(r.business_line) ==1:
                businessLine_ = 'Comercial'
            elif int(r.business_line) ==2:
                businessLine_ = 'Personal'
            elif int(r.business_line) ==0:
                businessLine_ = 'Otro'
            else:
                try:
                    if int(r.business_line) ==0:
                        businessLine_ = 'Otro'
                    else:
                        businessLine_ = ''
                except:
                    businessLine_ = ''
        else:
            businessLine_ = ''
        if r.contractor:
            contratan = ((((((r.contractor.full_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
            contratanE = r.contractor.email
            contratanP = r.contractor.phone_number
            if r.contractor.classification:
                clasifica_ = r.contractor.classification.classification_name  
            else:
                clasifica_='-----'
            if r.celula:
                cel = r.celula.celula_name  
            else:
                cel='-----'
            contratanG = ((((((r.contractor.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
            try:
                if r.contractor.group.type_group == 1:
                    contratanG = ((((((r.contractor.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    grupo1_ = ''
                    grupo2_ = ''
                elif r.contractor.group.type_group == 2:
                    grupotype1 = Group.objects.get(pk = r.contractor.group.parent.id)
                    contratanG = ((((((grupotype1.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    grupo1_ = ((((((r.contractor.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    grupo2_ = ''
                elif r.contractor.group.type_group == 3:
                    grupotype1 = Group.objects.get(pk = r.contractor.group.parent.parent.id)
                    contratanG = ((((((grupotype1.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    subgrupotype2 = Group.objects.get(pk = r.contractor.group.parent.id)
                    grupo1_ = ((((((subgrupotype2.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    grupo2_ = ((((((r.contractor.group.group_name).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
            except Exception as e:
                contratanG = ''
                grupo1_ = ''
                grupo2_ = ''
            try:
                if r.groupinglevel:
                    if r.groupinglevel.type_grouping == 1:
                        nivelAg_ = ((((((r.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo3_ = ''
                        grupo4_ = ''
                    elif r.groupinglevel.type_grouping == 2:
                        grupotype1 = GroupingLevel.objects.get(pk = r.groupinglevel.parent.id)
                        nivelAg_ = ((((((grupotype1.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo3_ = ((((((r.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo4_ = ''
                    elif r.groupinglevel.type_grouping == 3:
                        grupotype1 = GroupingLevel.objects.get(pk = r.groupinglevel.parent.parent.id)
                        nivelAg_ = ((((((grupotype1.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        subgrupotype2 = GroupingLevel.objects.get(pk = r.groupinglevel.parent.id)
                        grupo3_ = ((((((subgrupotype2.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                        grupo4_ = ((((((r.groupinglevel.description).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                else:
                    nivelAg_ = ''
                    grupo3_ = ''
                    grupo4_ = ''  
            except Exception as e:
                nivelAg_ = ''
                grupo3_ = ''
                grupo4_ = ''

            contacto = ContactInfo.objects.filter(contractor=r.contractor)
            if contacto:
                contacto_nombre = contacto[0].name
                contacto_telefono = contacto[0].phone_number
                contacto_email = contacto[0].email
            else:
                contacto_nombre = ''
                contacto_telefono = ''
                contacto_email = ''
        else:
            contratan = ''
            contratanE = ''
            contratanP = ''
            clasifica_='-----'                
            cel='-----'
            contratanG = ''                
            grupo1_ = ''
            grupo2_ = ''
            nivelAg_ = ''
            grupo3_ = ''
            grupo4_ = '' 
            contacto_nombre = ''
            contacto_telefono = ''
            contacto_email = ''
        try:
            old = OldPolicies.objects.filter(base_policy__id = r.id)
            try:
              date_renovacion = old[0].created_at.strftime("%d/%m/%Y")
            except Exception as rr:
                date_renovacion = 'Por renovar'
            try:
                ot_renovacion = old[0].new_policy.poliza_number if old[0].new_policy.poliza_number else old[0].new_policy.internal_number
            except Exception as rr:
                ot_renovacion = 'Por renovar'
        except Exception as dwe:
            date_renovacion = 'Por renovar'
            ot_renovacion = 'Por renovar'
        try:
            refs_policy = ReferenciadoresInvolved.objects.filter(policy = r.id,is_changed=False).order_by('referenciador__first_name').values_list('referenciador__first_name','referenciador__last_name')
            if len(refs_policy):
                v = []
                try:
                    for it in refs_policy:
                        v.append(it[0] +' '+ str(it[1])+str('; '))
                    referenc = v
                except Exception as y:
                    referenc = []
            else:
                referenc = []
        except Exception as e:
            referenc = []
        car_serial = ''        
        if r.ramo.ramo_code == 1:
            tasegurado = 'Titular Vida'
            form = Life.objects.filter(policy = r.id)
            if form:
                try:
                    if form[0].personal:
                        value = form[0].personal.full_name
                    else:
                        value = ''
                except Exception as e:
                    value = ''
                valueM = '' 
                valueMod = '' 
                valueV = '' 
                valueY = ''
                valueLP = '' 
                valueE = '' 
                valueAd = '' 
                valueDr = ''
            else:
                value = ''
                valueM = '' 
                valueMod = '' 
                valueV = '' 
                valueY = ''
                valueLP = '' 
                valueE = '' 
                valueAd = '' 
                valueDr = ''
        elif r.ramo.ramo_code == 2:
            tasegurado = 'Titular Accidentes'
            form = AccidentsDiseases.objects.filter(policy = r.id)
            if form:
                value = form[0].personal.full_name
                valueM = '' 
                valueMod = '' 
                valueV = '' 
                valueY = ''
                valueLP = '' 
                valueE = '' 
                valueAd = '' 
                valueDr = ''
            else:
                value = ''
                valueM = '' 
                valueMod = '' 
                valueV = '' 
                valueY = ''
                valueLP = '' 
                valueE = '' 
                valueAd = '' 
                valueDr = ''
        elif r.subramo.subramo_code == 9:
            tasegurado = 'Automóviles'
            form = AutomobilesDamages.objects.filter(policy = r.id)
            if form:
                try:
                    form[0].model = ((((((form[0].model).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                    form[0].version = ((((((form[0].version).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')
                except Exception as ers:
                    form[0].model = form[0].model
                    form[0].version = form[0].version

                value = str(form[0].brand) + '-' + str(form[0].model )+ '-' + str(form[0].version)
                valueM = form[0].brand 
                valueMod = form[0].model 
                valueV = form[0].version 
                valueY = form[0].year 
                valueLP = form[0].license_plates 
                valueE = form[0].engine 
                valueAd = form[0].adjustment 
                valueDr = form[0].driver

                car_serial = form[0].serial 
            else:
                value = ''
                valueM = '' 
                valueMod = '' 
                valueV = '' 
                valueY = ''
                valueLP = '' 
                valueE = '' 
                valueAd = '' 
                valueDr = ''
        elif r.ramo.ramo_code == 3 and not r.subramo.subramo_code == 9:
            form = Damages.objects.filter(policy = r.id)
            tasegurado = 'Bien asegurado'
            if form:
                value = form[0].insured_item
                valueM = '' 
                valueMod = '' 
                valueV = '' 
                valueY = ''
                valueLP = '' 
                valueE = '' 
                valueAd = '' 
                valueDr = ''
            else:
                value = ''
                valueM = '' 
                valueMod = '' 
                valueV = '' 
                valueY = ''
                valueLP = '' 
                valueE = '' 
                valueAd = '' 
                valueDr = ''
        if value:
            value = ((((((value).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'') 
        else:
            value = value
        incP = r.start_of_validity
        fnP =r.end_of_validity
        try:
            try:
                formatted = fnP.strftime('%H/%M/%S')
                # if formatted =='00/00/00' or formatted =='18/00/00' or formatted =='05/00/00':
                end_date =  (str(fnP).split('.')[0])
                end_date = fnP+ timedelta(hours=6, minutes=00)
                fnP = end_date
                fnP = fnP
            except Exception as eft:
                fnP =r.end_of_validity
        except:
            fnP = ''

        try:
            try:
                formatted = incP.strftime('%H/%M/%S')
                # if formatted =='00/00/00' or formatted =='18/00/00' or formatted =='05/00/00':
                end_date =  (str(incP).split('.')[0])
                end_date = incP+ timedelta(hours=6, minutes=00)
                incP = end_date
                incP = incP
            except Exception as eft:
                incP = r.start_of_validity
        except:
            incP = ''
   
        nueva.append('')
        asegurado.append(value)
        dateRen.append(date_renovacion)
        otRen.append(ot_renovacion)
        referenciador.append(referenc)
        antiguedad.append(antig)
        creadopor.append(val_owner)
        contratante.append(contratan)
        contratanteE.append(contratanE)
        contratanteP.append(contratanP)
        contratanteG.append(contratanG)        
        clave.append(cve)
        celulaC.append(cel)
        businessLine.append(businessLine_)
        cars_serials.append(car_serial)                   
        fechac.append(fec)            
        adjuntos.append(adj)            
        comisionPercent.append(comp)
        
        tipo_registro.append(checkDocumentType(r.document_type))
        tipoasegurado.append(tasegurado)
        num_poliza.append(r.poliza_number)
        internal.append(r.internal_number)
        asg.append(provider)
        rm.append(ramo_)
        srm.append(subramo_)
        identifier.append(identifi)
        pneta.append(r.p_neta)
        ptotal.append(r.p_total)
        rpf.append(r.rpf)
        derecho.append(r.derecho)
        iva.append(r.iva)
        comision.append(r.comision)
        comisionp.append(r.comision_percent)
        idpoliza.append(r.id)
        descuento.append(r.descuento)
        fpago.append(checkPayForm(r.forma_de_pago))
        # estado.append(checkStatusPolicy(r.status))
        estado.append(statusr)
        inivig.append(incP)
        endvig.append(fnP)
        moneda.append(checkCurrency(r.f_currency))
        creada.append(r.created_at)
        observac.append(desc)
        numendosos.append('')
        endosoconcepto.append('')
        tipodato.append('PÓLIZA')
        # --
        serierecibo.append('')
        estatusrecibo.append('')
        iniciorecibo.append('')
        finrecibo.append('')
        fechaconciliacion.append('')
        foliopago.append('')
        folioliquidacion.append('')
        comisionconciliada.append('')
        pagadopor.append('')
        liquidadopor.append('')
        recibosvencidos.append('')

        vencimiento.append('') 
        pnetarecibo.append('')  
        rpfrecibo.append('')
        derechorecibo.append('')
        ivarecibo .append('')
        ptotalrecibo.append('')
        comisionrecibo.append('')
        fechapago.append('')

        balancepneta.append('')
        balanceptotal.append('')
        recpagados = Recibos.objects.filter(receipt_type__in=[1,2,3],poliza = r, org_name=r.org_name, status__in=[1,5,6,7], 
            isCopy =False,isActive=True).aggregate(Sum('prima_total'))
        total_pag=(recpagados['prima_total__sum'] if 'prima_total__sum' in recpagados and recpagados['prima_total__sum'] !=None else 0)
        totalpagado.append(total_pag)        
        recpendientes = Recibos.objects.filter(receipt_type__in=[1,2,3],poliza = r, org_name=r.org_name, status__in=[4,3], 
            isCopy =False,isActive=True).aggregate(Sum('prima_total'))
        total_pend=(recpendientes['prima_total__sum'] if 'prima_total__sum' in recpendientes and recpendientes['prima_total__sum'] !=None else 0)
        totalpendiente.append(total_pend)
        reccancelados = Recibos.objects.filter(receipt_type__in=[1,2,3],poliza = r, org_name=r.org_name, status__in=[2], 
            isCopy =False,isActive=True).aggregate(Sum('prima_total'))
        total_canc=Decimal(reccancelados['prima_total__sum'] if 'prima_total__sum' in reccancelados and reccancelados['prima_total__sum'] != None else 0)
        totalcancelado.append(total_canc)

        otstramite.append('')
        balComRecPol = Recibos.objects.filter(poliza__id = r.id, isActive=True, isCopy=False, 
            receipt_type__in=[1]).exclude(status__in=[0,2]).aggregate(Sum('comision'))

        balancecom =Decimal(r.comision if r.comision else 0) - Decimal(balComRecPol['comision__sum'] if 'comision__sum' in balComRecPol and balComRecPol['comision__sum'] !=None else 0)
        balancepolizarecibos.append(balancecom)
        try:
            tiempovc=r.start_of_validity - r.created_at
            tiempovc= tiempovc.days
        except Exception as es:
            tiempovc= 0
            print('eror tiempo vig creacion',es)
        tiempovigenciacreacion.append(tiempovc)
        tiemporegistropago.append('')
        tiempoliquidacion.append('')
        idrecibo.append('')

        # sacar recibos de la poliza
        recibospoliza = Recibos.objects.filter(poliza = r, receipt_type=1, isCopy =False,isActive=True, org_name=r.org_name).exclude(status =0)
        recibospoliza2 = Recibos.objects.filter(poliza = r, receipt_type=3, isCopy =False,isActive=True, org_name=r.org_name,endorsement=None).exclude(status =0)
        if recibospoliza:
            for ir,rec in enumerate(recibospoliza):
                conteo_tipo.append(idx+1)
                if rec.receipt_type ==1:
                    tipodato.append('RECIBO PÓLIZA')
                else:
                    tipodato.append('NOTA CRÉDITO LIBRE')
                num_poliza.append(r.poliza_number)
                numendosos.append('')
                endosoconcepto.append('')
                contratante.append(r.contractor.full_name if r.contractor else '')
                contratanteE.append(r.contractor.email if r.contractor else '')
                contratanteP.append(r.contractor.phone_number if r.contractor else '')
                provider.append(r.aseguradora.alias if r.aseguradora else '')
                clave.append(str(r.clave.name) +'-'+str(r.clave.clave) if r.clave else '')
                ramo_.append(r.ramo.ramo_name if r.ramo else '')
                subramo_.append(r.subramo.subramo_name if r.subramo else '')
                tipo_registro.append(tasegurado)
                asegurado.append(value)
                estado.append(statusr)
                inivig.append(incP)
                endvig.append(fnP)
                fpago.append(checkPayForm(r.forma_de_pago))
                referenciador.append(referenc)
                moneda.append(checkCurrency(r.f_currency))
                conducto_de_pago.append(r.get_conducto_de_pago_display())
                pneta.append('')
                descuento.append('')
                rpf.append('')
                derecho.append('')
                iva.append('')
                ptotal.append('')
                comision.append('')
                comisionp.append('')
                idpoliza.append(r.id)
                serierecibo.append(rec.recibo_numero)
                estatusrecibo.append(rec.get_status_display())
                fir = rec.fecha_inicio
                try:
                    formatted = rec.fecha_inicio.strftime('%H/%M/%S')
                    # if formatted =='00/00/00' or formatted =='18/00/00' or formatted =='05/00/00':
                    fir_ =  (str(rec.fecha_inicio).split('.')[0])
                    fir_ = rec.fecha_inicio+ timedelta(hours=6, minutes=00)
                    fir = fir_
                    fir = fir.strftime("%d/%m/%Y")
                except Exception as eft:
                    fir = rec.fecha_inicio
                ffr = rec.fecha_fin
                try:
                    formatted = rec.fecha_inicio.strftime('%H/%M/%S')
                    # if formatted =='00/00/00' or formatted =='18/00/00' or formatted =='05/00/00':
                    ffr_ =  (str(rec.fecha_fin).split('.')[0])
                    ffr_ = rec.fecha_inicio+ timedelta(hours=6, minutes=00)
                    ffr = ffr_
                    ffr = ffr.strftime("%d/%m/%Y")
                except Exception as eft:
                    ffr = rec.fecha_fin
                fvr = rec.vencimiento
                try:
                    formatted = rec.vencimiento.strftime('%H/%M/%S')
                    # if formatted =='00/00/00' or formatted =='18/00/00' or formatted =='05/00/00':
                    fvr_ =  (str(rec.vencimiento).split('.')[0])
                    fvr_ = rec.vencimiento+ timedelta(hours=6, minutes=00)
                    fvr = fvr_
                    fvr = fvr.strftime("%d/%m/%Y")
                except Exception as eft:
                    fvr = rec.vencimiento
                iniciorecibo.append(fir)
                finrecibo.append(ffr)

                vencimiento.append(fvr) 
                pnetarecibo.append(rec.prima_neta)  
                rpfrecibo.append(rec.rpf)
                derechorecibo.append(rec.derecho)
                ivarecibo .append(rec.iva)
                ptotalrecibo.append(rec.prima_total)
                comisionrecibo.append(rec.comision)
                fechapago.append(rec.pay_date)

                fechaconciliacion.append(rec.conciliacion_date)
                foliopago.append(rec.folio_pago)
                folioliquidacion.append(rec.liquidacion_folio)
                comisionconciliada.append(rec.comision_conciliada)
                creada.append(rec.created_at)
                creadopor.append(str(rec.owner.first_name) + ' '+str(rec.owner.last_name) if rec.owner else '')
                pagadopor.append(str(rec.user_pay.first_name) + ' '+str(rec.user_pay.last_name) if rec.user_pay else '')
                liquidadopor.append(str(rec.user_liq.first_name) + ' '+str(rec.user_liq.last_name) if rec.user_liq else '')
                comments =  Comments.objects.filter(id_model=rec.id, model=4).order_by('-id')
                if comments:
                    com = comments[0].content
                    if com:
                        com = com.replace(',', '')
                        try:                
                            com = ((((((com).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'') 
                        except:
                            com = ''
                    bitacora.append(com)
                else:
                    bitacora.append('')  
                # bitacora
                if RecibosFile.objects.filter(owner__id = rec.id, org_name= rec.org_name).exists():
                    adjuntos.append('Si')
                else:
                    adjuntos.append('No')
                desc.append('')
                poliza_anterior.append('')
                poliza_siguiente.append('')
                balancepneta.append('')
                balanceptotal.append('')
                totalpagado.append('')
                totalpendiente.append('')
                totalcancelado.append('')
                otstramite.append('')
                if rec.fecha_fin:
                    if rec.status ==4 and rec.fecha_fin < datetime.today():
                        recibosvencidos.append('Si')
                    else:
                        recibosvencidos.append('No')
                else:
                    recibosvencidos.append('No')
                balancepolizarecibos.append('')
                try:
                    tiempovc_r=r.start_of_validity - r.created_at
                    tiempovc_r= tiempovc_r.days
                except Exception as es:
                    tiempovc_r= 0
                    print('eror tiempo vigrec creacion',es)
                tiempovigenciacreacion.append(tiempovc_r)
                tiemporegpago = ''
                try:
                    if rec.pay_date and rec.status in [1,5,6,7]:
                        logs = Log.objects.filter(Q(identifier__icontains='actualizó el recibo a pagado') | Q(identifier__icontains='con el folio de pag'),associated_id = rec.id).order_by('created_at')
                        if logs.exists():
                            datepago = logs[0]
                            tiemporegpago = rec.pay_date-datepago.created_at
                            tiemporegpago = tiemporegpago.days
                except Exception as tr:
                    print('erorr pago y APLICACIÓN',tr)
                tiemporegistropago.append(tiemporegpago)
                tiemporegliquidacion = ''
                try:
                    if rec.liquidacion_date and rec.status in [1,5,6,7]:
                        logs = Log.objects.filter(Q(identifier__icontains='actualizó el recibo a liquidado') | Q(identifier__icontains='con el folio de liquidación'),associated_id = rec.id).order_by('created_at')
                        if logs.exists():
                            datepago = logs[0]
                            tiemporegliquidacion = rec.liquidacion_date-datepago.created_at
                            tiemporegliquidacion = tiemporegliquidacion.days
                except Exception as tr:
                    print('erorr liqu y APLICACIÓN',tr)
                tiempoliquidacion.append(tiemporegliquidacion)
                idrecibo.append(rec.id)
        # nota de crédito
        if recibospoliza2:
            for ir,rec in enumerate(recibospoliza2):
                conteo_tipo.append(idx+1)
                tipodato.append('NOTA CRÉDITO LIBRE')
                num_poliza.append(r.poliza_number)
                numendosos.append('')
                endosoconcepto.append('')
                contratante.append(r.contractor.full_name if r.contractor else '')
                contratanteE.append(r.contractor.email if r.contractor else '')
                contratanteP.append(r.contractor.phone_number if r.contractor else '')
                provider.append(r.aseguradora.alias if r.aseguradora else '')
                clave.append(str(r.clave.name) +'-'+str(r.clave.clave) if r.clave else '')
                ramo_.append(r.ramo.ramo_name if r.ramo else '')
                subramo_.append(r.subramo.subramo_name if r.subramo else '')
                tipo_registro.append(tasegurado)
                asegurado.append(value)
                estado.append(statusr)
                inivig.append(incP)
                endvig.append(fnP)
                fpago.append(checkPayForm(r.forma_de_pago))
                referenciador.append(referenc)
                moneda.append(checkCurrency(r.f_currency))
                conducto_de_pago.append(r.get_conducto_de_pago_display())
                pneta.append('')
                descuento.append('')
                rpf.append('')
                derecho.append('')
                iva.append('')
                ptotal.append('')
                comision.append('')
                comisionp.append('')
                idpoliza.append(r.id)
                serierecibo.append(rec.recibo_numero)
                estatusrecibo.append(rec.get_status_display())
                fir = rec.fecha_inicio
                try:
                    formatted = rec.fecha_inicio.strftime('%H/%M/%S')
                    # if formatted =='00/00/00' or formatted =='18/00/00' or formatted =='05/00/00':
                    fir_ =  (str(rec.fecha_inicio).split('.')[0])
                    fir_ = rec.fecha_inicio+ timedelta(hours=6, minutes=00)
                    fir = fir_
                    fir = fir.strftime("%d/%m/%Y")
                except Exception as eft:
                    fir = rec.fecha_inicio
                ffr = rec.fecha_fin
                try:
                    formatted = rec.fecha_inicio.strftime('%H/%M/%S')
                    # if formatted =='00/00/00' or formatted =='18/00/00' or formatted =='05/00/00':
                    ffr_ =  (str(rec.fecha_fin).split('.')[0])
                    ffr_ = rec.fecha_inicio+ timedelta(hours=6, minutes=00)
                    ffr = ffr_
                    ffr = ffr.strftime("%d/%m/%Y")
                except Exception as eft:
                    ffr = rec.fecha_fin
                fvr = rec.vencimiento
                try:
                    formatted = rec.vencimiento.strftime('%H/%M/%S')
                    # if formatted =='00/00/00' or formatted =='18/00/00' or formatted =='05/00/00':
                    fvr_ =  (str(rec.vencimiento).split('.')[0])
                    fvr_ = rec.vencimiento+ timedelta(hours=6, minutes=00)
                    fvr = fvr_
                    fvr = fvr.strftime("%d/%m/%Y")
                except Exception as eft:
                    fvr = rec.vencimiento
                iniciorecibo.append(fir)
                finrecibo.append(ffr)

                vencimiento.append(fvr) 
                pnetarecibo.append(rec.prima_neta)  
                rpfrecibo.append(rec.rpf)
                derechorecibo.append(rec.derecho)
                ivarecibo .append(rec.iva)
                ptotalrecibo.append(rec.prima_total)
                comisionrecibo.append(rec.comision)
                fechapago.append(rec.pay_date)

                fechaconciliacion.append(rec.conciliacion_date)
                foliopago.append(rec.folio_pago)
                folioliquidacion.append(rec.liquidacion_folio)
                comisionconciliada.append(rec.comision_conciliada)
                creada.append(rec.created_at)
                creadopor.append(str(rec.owner.first_name) + ' '+str(rec.owner.last_name) if rec.owner else '')
                pagadopor.append(str(rec.user_pay.first_name) + ' '+str(rec.user_pay.last_name) if rec.user_pay else '')
                liquidadopor.append(str(rec.user_liq.first_name) + ' '+str(rec.user_liq.last_name) if rec.user_liq else '')
                comments =  Comments.objects.filter(id_model=rec.id, model=4).order_by('-id')
                if comments:
                    com = comments[0].content
                    if com:
                        com = com.replace(',', '')
                        try:                
                            com = ((((((com).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'') 
                        except:
                            com = ''
                    bitacora.append(com)
                else:
                    bitacora.append('')  
                # bitacora
                if RecibosFile.objects.filter(owner__id = rec.id, org_name= rec.org_name).exists():
                    adjuntos.append('Si')
                else:
                    adjuntos.append('No')
                desc.append('')
                poliza_anterior.append('')
                poliza_siguiente.append('')
                balancepneta.append('')
                balanceptotal.append('')
                totalpagado.append('')
                totalpendiente.append('')
                totalcancelado.append('')
                otstramite.append('')
                if rec.fecha_fin:
                    if rec.status ==4 and rec.fecha_fin < datetime.today():
                        recibosvencidos.append('Si')
                    else:
                        recibosvencidos.append('No')
                else:
                    recibosvencidos.append('No')
                balancepolizarecibos.append('')
                try:
                    tiempovc_r=r.start_of_validity - r.created_at
                    tiempovc_r= tiempovc_r.days
                except Exception as es:
                    tiempovc_r= 0
                    print('eror tiempo vigrec creacion',es)
                tiempovigenciacreacion.append(tiempovc_r)
                tiemporegpago = ''
                try:
                    if rec.pay_date and rec.status in [1,5,6,7]:
                        logs = Log.objects.filter(Q(identifier__icontains='actualizó el recibo a pagado') | Q(identifier__icontains='con el folio de pag'),associated_id = rec.id).order_by('created_at')
                        if logs.exists():
                            datepago = logs[0]
                            tiemporegpago = rec.pay_date-datepago.created_at
                            tiemporegpago = tiemporegpago.days
                except Exception as tr:
                    print('erorr pago y APLICACIÓN',tr)
                tiemporegistropago.append(tiemporegpago)
                tiemporegliquidacion = ''
                try:
                    if rec.liquidacion_date and rec.status in [1,5,6,7]:
                        logs = Log.objects.filter(Q(identifier__icontains='actualizó el recibo a liquidado') | Q(identifier__icontains='con el folio de liquidación'),associated_id = rec.id).order_by('created_at')
                        if logs.exists():
                            datepago = logs[0]
                            tiemporegliquidacion = rec.liquidacion_date-datepago.created_at
                            tiemporegliquidacion = tiemporegliquidacion.days
                except Exception as tr:
                    print('erorr liqu y APLICACIÓN',tr)
                tiempoliquidacion.append(tiemporegliquidacion)
                idrecibo.append(rec.id)
        # sacar endosos de la poliza y recibos de endosos
        endosospoliza = Endorsement.objects.filter(org_name =r.org_name, policy=r).exclude(status__in=[0])

        if endosospoliza and pre ==1:
            for ie, end in enumerate(endosospoliza):
                conteo_tipo.append(idx+1)
                tipodato.append('ENDOSO')
                num_poliza.append(r.poliza_number)
                numendosos.append(end.number_endorsement)               
                endosoconcepto.append(checkConceptEndoso(int(end.concept)))
                contratante.append(r.contractor.full_name if r.contractor else '')
                contratanteE.append(r.contractor.email if r.contractor else '')
                contratanteP.append(r.contractor.phone_number if r.contractor else '')
                provider.append(r.aseguradora.alias if r.aseguradora else '')
                clave.append(str(r.clave.name) +'-'+str(r.clave.clave) if r.clave else '')
                ramo_.append(r.ramo.ramo_name if r.ramo else '')
                subramo_.append(r.subramo.subramo_name if r.subramo else '')
                tipo_registro.append(tasegurado)
                asegurado.append(value)
                estado.append(statusr)
                inivig.append(end.init_date)
                endvig.append(end.end_date)
                fpago.append(checkPayForm(r.forma_de_pago))
                referenciador.append(referenc)
                moneda.append(checkCurrency(r.f_currency))
                conducto_de_pago.append(r.get_conducto_de_pago_display())
                pneta.append(end.p_neta)
                descuento.append(end.descuento)
                rpf.append(end.rpf)
                derecho.append(end.derecho)
                iva.append(end.iva)
                ptotal.append(end.p_total)
                comision.append(end.comision)
                comisionp.append('')
                idpoliza.append(r.id)
                serierecibo.append('')
                estatusrecibo.append('')
                iniciorecibo.append('')
                finrecibo.append('')

                vencimiento.append('') 
                pnetarecibo.append('')  
                rpfrecibo.append('')
                derechorecibo.append('')
                ivarecibo .append('')
                ptotalrecibo.append('')
                comisionrecibo.append('')
                fechapago.append('')

                fechaconciliacion.append('')
                foliopago.append('')
                folioliquidacion.append('')
                comisionconciliada.append('')
                creada.append(end.created_at)
                creadopor.append(str(end.owner.first_name) + ' '+str(end.owner.last_name) if end.owner else '')
                pagadopor.append('')
                liquidadopor.append('')
                comments =  Comments.objects.filter(id_model=end.id, model=10).order_by('-id')
                if comments:
                    com = comments[0].content
                    if com:
                        com = com.replace(',', '')
                        try:                
                            com = ((((((com).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'') 
                        except:
                            com = ''
                    bitacora.append(com)
                else:
                    bitacora.append('')  
                # bitacora
                if EndorsementFile.objects.filter(owner__id = end.id, org_name= end.org_name).exists():
                    adjuntos.append('Si')
                else:
                    adjuntos.append('No')
                desc.append('')
                poliza_anterior.append('')
                poliza_siguiente.append('')
                balancepneta.append('')
                balanceptotal.append('')
                totalpagado.append('')
                totalpendiente.append('')
                totalcancelado.append('')
                otstramite.append('')
                recibosvencidos.append('')
                balancepolizarecibos.append('')
                try:
                    tiempovc_r=end.init_date - end.created_at
                    tiempovc_r= tiempovc_r.days
                except Exception as es:
                    tiempovc_r= 0
                    print('eror tiempo vigrec creacion',es)
                tiempovigenciacreacion.append(tiempovc_r)
                tiemporegpago = ''
                tiemporegistropago.append(tiemporegpago)
                tiemporegliquidacion = ''
                tiempoliquidacion.append(tiemporegliquidacion)
                idrecibo.append('')
                # recibos de endosos
                recibosendosos = Recibos.objects.filter(endorsement = end, poliza = r, receipt_type__in=[2,3], isCopy =False,isActive=True, 
                    org_name=r.org_name).exclude(status =0)
                if recibosendosos:
                    for ir,rec in enumerate(recibosendosos):
                        conteo_tipo.append(idx+1)
                        tipodato.append('RECIBO ENDOSO')
                        num_poliza.append(r.poliza_number)
                        numendosos.append(end.number_endorsement)
                        endosoconcepto.append('')
                        contratante.append(r.contractor.full_name if r.contractor else '')
                        contratanteE.append(r.contractor.email if r.contractor else '')
                        contratanteP.append(r.contractor.phone_number if r.contractor else '')
                        provider.append(r.aseguradora.alias if r.aseguradora else '')
                        clave.append(str(r.clave.name) +'-'+str(r.clave.clave) if r.clave else '')
                        ramo_.append(r.ramo.ramo_name if r.ramo else '')
                        subramo_.append(r.subramo.subramo_name if r.subramo else '')
                        tipo_registro.append(tasegurado)
                        asegurado.append(value)
                        estado.append(statusr)
                        inivig.append(incP)
                        endvig.append(fnP)
                        fpago.append(checkPayForm(r.forma_de_pago))
                        referenciador.append(referenc)
                        moneda.append(checkCurrency(r.f_currency))
                        conducto_de_pago.append(r.get_conducto_de_pago_display())
                        pneta.append('')
                        descuento.append('')
                        rpf.append('')
                        derecho.append('')
                        iva.append('')
                        ptotal.append('')
                        comision.append('')
                        comisionp.append('')
                        idpoliza.append(r.id)
                        serierecibo.append(rec.recibo_numero)
                        estatusrecibo.append(rec.get_status_display())
                        fir = rec.fecha_inicio
                        try:
                            formatted = rec.fecha_inicio.strftime('%H/%M/%S')
                            # if formatted =='00/00/00' or formatted =='18/00/00' or formatted =='05/00/00':
                            fir_ =  (str(rec.fecha_inicio).split('.')[0])
                            fir_ = rec.fecha_inicio+ timedelta(hours=6, minutes=00)
                            fir = fir_
                            fir = fir.strftime("%d/%m/%Y")
                        except Exception as eft:
                            fir = rec.fecha_inicio
                        ffr = rec.fecha_fin
                        try:
                            formatted = rec.fecha_inicio.strftime('%H/%M/%S')
                            # if formatted =='00/00/00' or formatted =='18/00/00' or formatted =='05/00/00':
                            ffr_ =  (str(rec.fecha_fin).split('.')[0])
                            ffr_ = rec.fecha_inicio+ timedelta(hours=6, minutes=00)
                            ffr = ffr_
                            ffr = ffr.strftime("%d/%m/%Y")
                        except Exception as eft:
                            ffr = rec.fecha_fin
                        fvr = rec.vencimiento
                        try:
                            formatted = rec.vencimiento.strftime('%H/%M/%S')
                            # if formatted =='00/00/00' or formatted =='18/00/00' or formatted =='05/00/00':
                            fvr_ =  (str(rec.vencimiento).split('.')[0])
                            fvr_ = rec.vencimiento+ timedelta(hours=6, minutes=00)
                            fvr = fvr_
                            fvr = fvr.strftime("%d/%m/%Y")
                        except Exception as eft:
                            fvr = rec.vencimiento
                        iniciorecibo.append(fir)
                        finrecibo.append(ffr)

                        vencimiento.append(fvr) 
                        pnetarecibo.append(rec.prima_neta)  
                        rpfrecibo.append(rec.rpf)
                        derechorecibo.append(rec.derecho)
                        ivarecibo .append(rec.iva)
                        ptotalrecibo.append(rec.prima_total)
                        comisionrecibo.append(rec.comision)
                        fechapago.append(rec.pay_date)

                        fechaconciliacion.append(rec.conciliacion_date)
                        foliopago.append(rec.folio_pago)
                        folioliquidacion.append(rec.liquidacion_folio)
                        comisionconciliada.append(rec.comision_conciliada)
                        creada.append(rec.created_at)
                        creadopor.append(str(rec.owner.first_name) + ' '+str(rec.owner.last_name) if rec.owner else '')
                        pagadopor.append(str(rec.user_pay.first_name) + ' '+str(rec.user_pay.last_name) if rec.user_pay else '')
                        liquidadopor.append(str(rec.user_liq.first_name) + ' '+str(rec.user_liq.last_name) if rec.user_liq else '')
                        comments =  Comments.objects.filter(id_model=rec.id, model=4).order_by('-id')
                        if comments:
                            com = comments[0].content
                            if com:
                                com = com.replace(',', '')
                                try:                
                                    com = ((((((com).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'') 
                                except:
                                    com = ''
                            bitacora.append(com)
                        else:
                            bitacora.append('')  
                        # bitacora
                        if RecibosFile.objects.filter(owner__id = rec.id, org_name= rec.org_name).exists():
                            adjuntos.append('Si')
                        else:
                            adjuntos.append('No')
                        desc.append('')
                        poliza_anterior.append('')
                        poliza_siguiente.append('')
                        balancepneta.append('')
                        balanceptotal.append('')
                        totalpagado.append('')
                        totalpendiente.append('')
                        totalcancelado.append('')
                        otstramite.append('')
                        if rec.status ==4 and rec.fecha_fin < datetime.today():
                            recibosvencidos.append('Si')
                        else:
                            recibosvencidos.append('No')
                        balancepolizarecibos.append('')
                        try:
                            tiempovc_r=r.start_of_validity - r.created_at
                            tiempovc_r= tiempovc_r.days
                        except Exception as es:
                            tiempovc_r= 0
                            print('eror tiempo vigrec creacion',es)
                        tiempovigenciacreacion.append(tiempovc_r)
                        tiemporegpago = ''
                        try:
                            if rec.pay_date and rec.status in [1,5,6,7]:
                                logs = Log.objects.filter(Q(identifier__icontains='actualizó el recibo a pagado') | Q(identifier__icontains='con el folio de pag'),associated_id = rec.id).order_by('created_at')
                                if logs.exists():
                                    datepago = logs[0]
                                    tiemporegpago = rec.pay_date-datepago.created_at
                                    tiemporegpago = tiemporegpago.days
                                    print('tiemporegpago',tiemporegpago)
                        except Exception as tr:
                            print('erorr pago y APLICACIÓN',tr)
                        tiemporegistropago.append(tiemporegpago)
                        tiemporegliquidacion = ''
                        try:
                            if rec.liquidacion_date and rec.status in [1,5,6,7]:
                                logs = Log.objects.filter(Q(identifier__icontains='actualizó el recibo a liquidado') | Q(identifier__icontains='con el folio de liquidación'),associated_id = rec.id).order_by('created_at')
                                if logs.exists():
                                    datepago = logs[0]
                                    tiemporegliquidacion = rec.liquidacion_date-datepago.created_at
                                    tiemporegliquidacion = tiemporegliquidacion.days
                        except Exception as tr:
                            print('erorr liqu y APLICACIÓN',tr)
                        tiempoliquidacion.append(tiemporegliquidacion)
                        idrecibo.append(rec.id)
            
        # --------------------------------
        if endosospoliza and pre ==1:
            endososTodos = Endorsement.objects.filter(org_name =r.org_name, policy=r).exclude(status__in=[0]).exclude(concept =1)
            recibosTodos = Recibos.objects.filter(poliza = r,isCopy =False,isActive=True, org_name=r.org_name).exclude(status =0)
        else:
            endososTodos =None
            # recibosTodos = Recibos.objects.filter(poliza = r,isCopy =False,isActive=True, endorsement=None org_name=r.org_name,receipt_type__in =[1]).exclude(status =0)
            recibosTodos = Recibos.objects.filter(poliza = r,isCopy =False,isActive=True, org_name=r.org_name,receipt_type__in =[1]).exclude(status =0)

        if endososTodos:
            balanceneta1=endososTodos.aggregate(Sum('p_neta'))
            balancetotal1=endososTodos.aggregate(Sum('p_total'))
            balancerpf1=endososTodos.aggregate(Sum('rpf'))
            balancederecho1=endososTodos.aggregate(Sum('rpf'))
            balanceiva1=endososTodos.aggregate(Sum('iva'))
            balancecomision1=endososTodos.aggregate(Sum('comision'))
            # ----------            
            balanceneta =Decimal(r.p_neta if r.p_neta else 0) + Decimal(balanceneta1['p_neta__sum'] if 'p_neta__sum' in balanceneta1 and balanceneta1['p_neta__sum'] !=None else 0)
            balancetotal =Decimal(r.p_total if r.p_total else 0) + Decimal(balancetotal1['p_total__sum'] if 'p_total__sum' in balancetotal1 and balancetotal1['p_total__sum'] !=None else 0)       
            balancerpf =Decimal(r.rpf if r.rpf else 0) + Decimal(balancerpf1['rpf__sum'] if 'rpf__sum' in balancerpf1 and balancerpf1['rpf__sum'] !=None else 0)            
            balancederecho =Decimal(r.derecho if r.derecho else 0) + Decimal(balancederecho1['derecho__sum'] if 'derecho__sum' in balancederecho1 and balancederecho1['derecho__sum'] !=None else 0)            
            balanceiva =Decimal(r.iva if r.iva else 0) + Decimal(balanceiva1['iva__sum'] if 'iva__sum' in balanceiva1 and balanceiva1['iva__sum'] !=None else 0)            
            balancecomision =Decimal(r.comision if r.comision else 0) + Decimal(balancecomision1['comision__sum'] if 'comision__sum' in balancecomision1 and balancecomision1['comision__sum'] !=None else 0)
        else:            
            balancetotal1=0
            balancerpf1=0
            balancederecho1=0
            balanceiva1=0
            balancecomision1=0
            # ---
            balanceneta =Decimal(r.p_neta if r.p_neta else 0)
            balancetotal =Decimal(r.p_total if r.p_total else 0)      
            balancerpf =Decimal(r.rpf if r.rpf else 0)     
            balancederecho =Decimal(r.derecho if r.derecho else 0)            
            balanceiva =Decimal(r.iva if r.iva else 0)          
            balancecomision =Decimal(r.comision if r.comision else 0)
        # ----------
        sumaspneta1=recibosTodos.aggregate(Sum('prima_neta'))
        sumaspriman = Decimal(sumaspneta1['prima_neta__sum'] if 'prima_neta__sum' in sumaspneta1 and sumaspneta1['prima_neta__sum'] !=None else 0)

        sumasptotal1=recibosTodos.aggregate(Sum('prima_total'))
        sumasprimat = Decimal(sumasptotal1['prima_total__sum'] if 'prima_total__sum' in sumasptotal1 and sumasptotal1['prima_total__sum'] !=None else 0)
        
        sumasrpf1=recibosTodos.aggregate(Sum('rpf'))
        sumasrpf = Decimal(sumasrpf1['rpf__sum'] if 'rpf__sum' in sumasrpf1 and sumasrpf1['rpf__sum'] !=None else 0)
        
        sumasderecho1=recibosTodos.aggregate(Sum('derecho'))
        sumasderecho = Decimal(sumasderecho1['derecho__sum'] if 'derecho__sum' in sumasderecho1 and sumasderecho1['derecho__sum'] !=None else 0)
        
        sumasiva1=recibosTodos.aggregate(Sum('iva'))
        sumasiva = Decimal(sumasiva1['iva__sum'] if 'iva__sum' in sumasiva1 and sumasiva1['iva__sum'] !=None else 0)    
        
        sumasprima_total1=recibosTodos.aggregate(Sum('prima_total'))
        sumasprima_total = Decimal(sumasprima_total1['prima_total__sum'] if 'prima_total__sum' in sumasprima_total1 and sumasprima_total1['prima_total__sum'] !=None else 0)        
        
        sumascomision_conciliada1=recibosTodos.aggregate(Sum('comision_conciliada'))
        sumascomision_conciliada = Decimal(sumascomision_conciliada1['comision_conciliada__sum'] if 'comision_conciliada__sum' in sumascomision_conciliada1 and sumascomision_conciliada1['comision_conciliada__sum'] !=None else 0)

        sumascomision1=recibosTodos.aggregate(Sum('comision'))
        sumascomision = Decimal(sumascomision1['comision__sum'] if 'comision__sum' in sumascomision1 and sumascomision1['comision__sum'] !=None else 0)
        # ****************
        conteo_tipo.append(idx+1)
        tipodato.append('ANÁLISIS')
        num_poliza.append(r.poliza_number)
        numendosos.append('')
        endosoconcepto.append('')
        contratante.append(r.contractor.full_name if r.contractor else '')
        contratanteE.append(r.contractor.email if r.contractor else '')
        contratanteP.append(r.contractor.phone_number if r.contractor else '')
        provider.append(r.aseguradora.alias if r.aseguradora else '')
        clave.append(str(r.clave.name) +'-'+str(r.clave.clave) if r.clave else '')
        ramo_.append(r.ramo.ramo_name if r.ramo else '')
        subramo_.append(r.subramo.subramo_name if r.subramo else '')
        tipo_registro.append(tasegurado)
        asegurado.append(value)
        estado.append(statusr)
        inivig.append(incP)
        endvig.append(fnP)
        fpago.append(checkPayForm(r.forma_de_pago))
        referenciador.append(referenc)
        moneda.append(checkCurrency(r.f_currency))
        conducto_de_pago.append(r.get_conducto_de_pago_display())
        pneta.append(balanceneta)
        descuento.append('')
        rpf.append(balancerpf)
        derecho.append(balancederecho)
        iva.append(balanceiva)
        ptotal.append(balancetotal)
        comision.append(balancecomision)
        comisionp.append('')
        idpoliza.append(r.id)
        serierecibo.append('')
        estatusrecibo.append('')
        iniciorecibo.append('')
        finrecibo.append('')

        vencimiento.append('') 
        pnetarecibo.append(sumaspriman)  
        rpfrecibo.append(sumasrpf)
        derechorecibo.append(sumasderecho)
        ivarecibo .append(sumasiva)
        ptotalrecibo.append(sumasprimat)
        comisionrecibo.append(sumascomision)
        fechapago.append('')

        fechaconciliacion.append('')
        foliopago.append('')
        folioliquidacion.append('')
        comisionconciliada.append(sumascomision_conciliada)
        creada.append('')
        creadopor.append('')
        pagadopor.append('')
        liquidadopor.append('')
        bitacora.append('')  
        adjuntos.append('')
        desc.append('')
        poliza_anterior.append('')
        poliza_siguiente.append('')
        balancepneta.append(balanceneta - sumaspriman)
        balanceptotal.append(balancetotal - sumasprimat)
        totalpagado.append(0)
        totalpendiente.append(0)
        totalcancelado.append(0)
        otstramite.append('')
        recibosvencidos.append('')
        balancepolizarecibos.append('')
        tiempovigenciacreacion.append('')
        tiemporegpago = ''
        tiemporegistropago.append('')
        tiemporegliquidacion = ''
        tiempoliquidacion.append('')
        idrecibo.append('')
        # balnace final

    df = pd.DataFrame()
    # if 'REGISTRO' in columns:
    df['REGISTRO'] =conteo_tipo
    # if 'TIPO DE DATO' in columns:
    df['TIPO DE DATO'] =tipodato
    # if 'NUMERO DE POLIZA' in columns:
    df['NUMERO DE POLIZA'] = num_poliza
    # if 'ENDOSO' in columns:#*
    df['ENDOSO'] =numendosos
    df['CONCEPTO'] =endosoconcepto
    # if 'CONTRATANTE' in columns:
    df['CONTRATANTE'] =contratante
    # if 'CORREO' in columns:
    df['CORREO']=contratanteE
    # if 'TELEFONO' in columns:
    df['TELEFONO'] =  contratanteP
    # if 'ASEGURADORA' in columns:
    df['ASEGURADORA'] =provider
    # if 'CLAVE' in columns:
    df['CLAVE'] =clave
    # if 'RAMO' in columns:
    df['RAMO']=ramo_
    # if 'SUBRAMO' in columns:
    df['SUBRAMO'] =subramo_
    # if 'TIPO' in columns:
    df['TIPO'] = tipo_registro
    # if 'BIEN ASEGURADO' in columns:
    df['BIEN ASEGURADO'] = asegurado
    # if 'ESTATUS' in columns:
    df['ESTATUS'] = estado
    # if 'INICIO VIGENCIA (POL/END)' in columns:
    df['INICIO VIGENCIA (POL/END)'] = inivig
    # if 'FIN VIGENCIA (POL/END)' in columns:
    df['FIN VIGENCIA (POL/END)'] =endvig
    # if 'FORMA DE PAGO' in columns:
    df['FORMA DE PAGO']= fpago
    # if 'REFERENCIADOR' in columns:

    if verReferenciadores =='True' or verReferenciadores==True:
        df['REFERENCIADOR'] = [str(x).replace('[','').replace(']','').replace('\'','').replace(';','') for x in referenciador]
    # if 'MONEDA' in columns:
    df['MONEDA'] = moneda
    # if 'CONDUCTO DE PAGO' in columns:
    df['CONDUCTO DE PAGO'] = conducto_de_pago
    # if 'PRIMA NETA' in columns:
    df['PRIMA NETA'] = pneta
    # if 'DESCUENTO' in columns:
    df['DESCUENTO'] =descuento
    # if 'RPF' in columns:
    df['RPF'] = rpf
    # if 'DERECHO' in columns:
    df['DERECHO'] =derecho
    # if 'IVA' in columns:
    df['IVA'] = iva
    # if 'PRIMA TOTAL' in columns:
    df['PRIMA TOTAL'] =ptotal
    # if'COMISION $' in columns:
    df['COMISION $'] = comision
    # if 'COMISION %' in columns:
    df['COMISION %'] = comisionp
    # if 'ID POLIZA' in columns:
    df['ID POLIZA'] = idpoliza
    # if 'SERIE RECIBO' in columns:
    df['SERIE RECIBO'] = serierecibo
    # if 'ESTATUS RECIBO' in columns:
    df['ESTATUS RECIBO'] = estatusrecibo
    # if 'Fecha Inicio recibo' in columns:
    df['Fecha Inicio recibo'] = iniciorecibo
    # if 'Fecha fin recibo' in columns:
    df['Fecha fin recibo'] = finrecibo

    # if 'Vencimiento' in columns:
    df['Vencimiento'] = vencimiento
    # if 'Prima Neta Recibo' in columns:
    df['Prima Neta Recibo'] = pnetarecibo
    # if 'RPF Recibo' in columns:
    df['RPF Recibo'] = rpfrecibo
    # if 'Derecho Recibo' in columns:
    df['Derecho Recibo'] = derechorecibo
    # if 'IVA Recibo' in columns:
    df['IVA Recibo'] = ivarecibo
    # if 'Prima Total Recibo' in columns:
    df['Prima Total Recibo'] = ptotalrecibo
    # if 'Comisión Recibo' in columns:
    df['Comisión Recibo'] = comisionrecibo
    # if 'Fecha Pago' in columns:
    df['Fecha Pago'] = fechapago 
        
    # if 'Fecha de conciliación' in columns:
    df['Fecha de conciliación'] =fechaconciliacion
    # if 'Folio de Pago' in columns:
    df['Folio de Pago'] =foliopago
    # if 'Folio de Liquidación' in columns:
    df['Folio de Liquidación'] = folioliquidacion 
    # if 'Comisión conciliada' in columns:
    df['Comisión conciliada'] =comisionconciliada 
    # if 'Fecha Creación' in columns:
    df['Fecha Creación'] =creada 
    # if 'Creado por' in columns:
    df['Creado por'] = creadopor 
    # if 'Pagado por' in columns:
    df['Pagado por'] =pagadopor 
    # if 'Liquidado por' in columns:
    df['Liquidado por'] =liquidadopor 
    # if 'Bitácora' in columns:
    df['Bitácora'] = bitacora
    # if 'Adjuntos' in columns:
    df['Adjuntos'] =adjuntos
    # if 'DESCRIPCION/OBSERVACIONES' in columns:
    df['DESCRIPCION/OBSERVACIONES'] =desc
    # if 'POLIZA ANTERIOR' in columns:
    df['POLIZA ANTERIOR']= poliza_anterior
    # if 'POLIZA SIGUIENTE' in columns:
    df['POLIZA SIGUIENTE'] = poliza_siguiente
    # if 'BALANCE PRIMA NETA' in columns:
    df['BALANCE PRIMA NETA'] = balancepneta

    # if 'BALANCE PRIMA TOTAL' in columns:
    df['BALANCE PRIMA TOTAL'] = balanceptotal
    # if 'TOTAL PAGADO' in columns:
    df['TOTAL PAGADO'] = totalpagado
    # if 'TOTAL PENDIENTE' in columns:
    df['TOTAL PENDIENTE'] = totalpendiente
    # if 'TOTAL CANCELADO' in columns:
    df['TOTAL CANCELADO'] = totalcancelado
    # if 'OTs EN TRAMITE' in columns:
    df['OTs EN TRAMITE'] = otstramite
    # if 'RECIBO VENCIDO' in columns:
    df['RECIBO VENCIDO'] = recibosvencidos
    # if 'BALANCE COMISION POLIZAvsRECIBOS' in columns:
    df['BALANCE COMISION POLIZAvsRECIBOS']= balancepolizarecibos
    # if 'TIEMPO INI. VIG./CREACION' in columns:
    df['TIEMPO INI. VIG./CREACION']= tiempovigenciacreacion
    # if 'SAAM TIEMPO PAGO/APLICACIÓN' in columns:
    df['SAAM TIEMPO PAGO/APLICACIÓN']= tiemporegistropago
    # if 'TIEMPO  LIQUIDACION/APLICACIÓN' in columns:
    df['TIEMPO  LIQUIDACION/APLICACIÓN']= tiempoliquidacion
    df['ID RECIBO'] = idrecibo

    try:
        df['Fecha Creación'] = pd.to_datetime(df['Fecha Creación'], errors='ignore', format='%D/%B/%Y').dt.date
    except Exception as e:
        df['Fecha Creación'] = df['Fecha Creación']
    try:
        df['Fecha de conciliación'] = pd.to_datetime(df['Fecha de conciliación'], errors='ignore', format='%D/%B/%Y').dt.date
    except Exception as e:
        df['Fecha de conciliación'] = df['Fecha de conciliación']
    try:
        df['Vencimiento'] = pd.to_datetime(df['Vencimiento'], errors='ignore', format='%D/%B/%Y').dt.date
    except Exception as e:
        df['Vencimiento'] = df['Vencimiento']
    try:
        df['Fecha Pago'] = pd.to_datetime(df['Fecha Pago'], errors='ignore', format='%D/%B/%Y').dt.date
    except Exception as e:
        df['Fecha Pago'] = df['Fecha Pago']
    try:
        df['INICIO VIGENCIA (POL/END)'] = pd.to_datetime(df['INICIO VIGENCIA (POL/END)'], errors='ignore', format='%D/%B/%Y').dt.date
    except Exception as e:
        df['INICIO VIGENCIA (POL/END)'] = df['INICIO VIGENCIA (POL/END)']

    try:
        df['FIN VIGENCIA (POL/END)'] = pd.to_datetime(df['FIN VIGENCIA (POL/END)'], errors='ignore', format='%D/%B/%Y').dt.date
    except Exception as e:
        df['FIN VIGENCIA (POL/END)'] =df['FIN VIGENCIA (POL/END)']

    try:
        df['Fecha Inicio recibo'] = pd.to_datetime(df['Fecha Inicio recibo'], errors='ignore', format='%D/%B/%Y').dt.date
    except Exception as e:
        df['Fecha Inicio recibo'] = df['Fecha Inicio recibo']

    try:
        df['Fecha fin recibo'] = pd.to_datetime(df['Fecha fin recibo'], errors='ignore', format='%D/%B/%Y').dt.date
    except Exception as e:
        df['Fecha fin recibo'] = df['Fecha fin recibo']
    try:
        df['PRIMA NETA'] =  pd.to_numeric(df['PRIMA NETA'], errors='ignore')
        df['DESCUENTO'] =  pd.to_numeric(df['DESCUENTO'], errors='ignore')
        df['RPF'] =  pd.to_numeric(df['RPF'], errors='ignore')
        df['DERECHO'] =  pd.to_numeric(df['DERECHO'], errors='ignore')
        df['IVA'] =  pd.to_numeric(df['IVA'], errors='ignore')
        df['PRIMA TOTAL'] =  pd.to_numeric(df['PRIMA TOTAL'], errors='ignore')
        df['COMISION $'] =  pd.to_numeric(df['COMISION $'], errors='ignore')
        #    -------------------
        df['Prima Neta Recibo'] =  pd.to_numeric(df['Prima Neta Recibo'], errors='ignore')
        df['RPF Recibo'] =  pd.to_numeric(df['RPF Recibo'], errors='ignore')
        df['Derecho Recibo'] =  pd.to_numeric(df['Derecho Recibo'], errors='ignore')
        df['IVA Recibo'] =  pd.to_numeric(df['IVA Recibo'], errors='ignore')
        df['Prima Total Recibo'] =  pd.to_numeric(df['Prima Total Recibo'], errors='ignore')
        df['Comisión Recibo'] =  pd.to_numeric(df['Comisión Recibo'], errors='ignore')
        # --------------------
        df['TOTAL PAGADO'] =  pd.to_numeric(df['TOTAL PAGADO'], errors='ignore')
        df['TOTAL PENDIENTE'] =  pd.to_numeric(df['TOTAL PENDIENTE'], errors='ignore')
        df['TOTAL CANCELADO'] =  pd.to_numeric(df['TOTAL CANCELADO'], errors='ignore')
        df['TOTAL CANCELADO'] =  pd.to_numeric(df['TOTAL CANCELADO'], errors='ignore')
        df['BALANCE PRIMA NETA'] =  pd.to_numeric(df['BALANCE PRIMA NETA'], errors='ignore')
        df['BALANCE PRIMA TOTAL'] =  pd.to_numeric(df['BALANCE PRIMA TOTAL'], errors='ignore')
    except Exception as e:
        print('erorr------------',e)
    try:
       df.style.format(na_rep="ANÁLISIS").highlight_null(background_color="red")
    except Exception as fd:
        print('fd--------------',fd)
    filename = 'reporte_auditoria_%s_%s_%s-%s.xlsx'%(org, self.request.id , str(since).replace('-','_').split('T')[0], str(until).replace('-','_').split('T')[0])
    writer = pd.ExcelWriter(filename, engine='xlsxwriter')
    # -------------------------------------------------------
    def bold_style(val):
        f = "font-weight: bold" 
        #condition
        m = val["TIPO DE DATO"] == 'ANÁLISIS'
        # DataFrame of styles
        df1 = pd.DataFrame('', index=val.index, columns=val.columns)
        # set columns by condition
        df1 = df1.mask(m, f)
        return df1
    # ----------------
    df.to_excel(writer, sheet_name='Sheet1', index=False)
    # Close the Pandas Excel writer and output the Excel file.
    writer.save()
    url_response = 'timeout'
    with open(filename) as f:
        url_response = upload_to_s3(filename, org)
        try:
            try:
                user_ownern = post.get('user_req')
                user_owner = User.objects.get(username = user_ownern)
            except Exception as uname:
                user_ownern = 'superuser_'+org
                user_owner = User.objects.get(username = user_ownern)
            # crear lanotificación
            payload = {
                "seen": False,
                "id_reference":0,
                "assigned":user_ownern,
                "org_name":org,
                "owner":user_ownern,
                "description":url_response,
                "model":26,
                "title":'El reporte (auditoría) solicitado ha sido generado'
            }
            r = requests.post(settings.API_URL + 'crear-notificacion-reportes/', (payload), verify=False)
        except Exception as er:
            print('no se creo al notificacion auditoria',er)
            pass
        print('url creada',url_response)
        redis_client.publish(self.request.id, url_response)  
    end_time = time.perf_counter()
    total_time = end_time - start_time
    try:
        logsystem = {'data':'Reporte fianza endosos vista','conteo':len(polizas),'org_name':org,
                    'user': post['user'], 'inicio':start_time,'fin':end_time,'total':total_time}
        logs =send_log(post['user'], org, 'GET', 32,json.dumps(logsystem),None)
    except Exception as j:
        print('--------',j)
    try:
        os.remove(filename)
        return 'Auditoria Detalle Excel Executed'
    except Exception as ers:
        os.remove(filename)
        return 'Auditoria Detalle Excel Executed'
# ------------

def highlight_col(column, bool_masks, color_list, default="white"):        
    cond_bg_style = [f'background-color: {color}' for color in color_list]
    default_bg_style = f'background-color:{default}'
    print('ddddddd',)
    return np.select(bool_masks, cond_bg_style, default=default_bg_style)

def checkConceptEndoso(request):
    switcher = {
        53: "Detalle de Garantía",
        54: "Prórroga de vigencia",
        58: "Aumento",
        59: "Disminución",
        60: "Anulación",
        61: "Cancelación",
        55: "Anuencia",
        4: "Endoso de texto/Otro",
        57: "Aumento y Prórroga",
        62: "Disminución y Prórroga",
        34: "Cambio en Beneficiario",
        24: "Alta de Certificados",
        25: "Baja de Certificados",
        56: "Correcciones de Certificados",
    }

def checkConceptEndoso(request):
    switcher = {
        53: "Detalle de Garantía",
        54: "Prórroga de vigencia",
        58: "Aumento",
        59: "Disminución",
        60: "Anulación",
        61: "Cancelación",
        55: "Anuencia",
        4: "Endoso de texto/Otro",
        57: "Aumento y Prórroga",
        62: "Disminución y Prórroga",
        34: "Cambio en Beneficiario",
        24: "Alta de Certificados",
        25: "Baja de Certificados",
        56: "Correcciones de Certificados",
        1: "CAMBIO DE FORMA DE PAGO",
        2: "CAMBIO DE SUMA ASEGURADA",
        3: 'CAMBIO DE DEDUCIBLE Y/O COASEGURO',
        4: 'PRÓRROGA DE VIGENCIA',
        5: 'AGREGAR COBERTURAS',
        6: 'DECLARACIÓN MENSUAL',
        8: 'CAMBIO DATOS FISCALES',
        9: 'ESPECIFICACIONES',
        11: 'CAMBIO DE FORMA DE PAGO',
        12: 'DISMINUCIÓN EN LA SUMA ASEGURADA',
        13: 'BAJA DE COBERTURA',
        14: 'CAMBIO DE DEDUCIBLE Y/O COASEGURO',
        15: 'BAJA DE ASEGURADO',
        16: 'CANCELACIÓN DE PÓLIZA POR PETICIÓN',
        17: 'CANCELACIÓN DE PÓLIZA POR FALTA DE PAGO',
        23: 'ANTIGÜEDAD RECONOCIDA',
        24: 'ALTA DE ASEGURADOS',
        25: 'BAJA DE ASEGURADOS',
        26: 'AJUSTE ANUAL (A)',
        27: 'RECONOCIMIENTO DE ANTIGUEDAD',
        28: 'AJUSTE ANUAL (D)',
        29: 'OTRO',
        30: 'Declaración',
        31: 'Agregar asegurado o dependiente',
        32: 'Cancelación',
        33: 'CORRECCION DE DATOS',
        34: 'CORRECCION DE DATOS BENEFICIARIO',
        41: 'ALTA DE UNIDAD',
        42: 'AGREGAR COBERTURA',
        43: 'CAMBIO DE FORMA DE PAGO',
        44: 'AGREGAR CARGA',
        45: 'AGREGAR ADAPTACIONES Y CONVERSIONES',
        46: 'INCLUIR BP',
        47: 'CORREGIR DESCRIPCIÓN',
        48: 'AGREGAR TEXTOS ACLARATORIOS',
        49: 'ADAPTACIONES SOLO PARA EFECTOS DE RC',
        50: 'AGREGAR PLACAS, MOTOR O CORREGIR SERIE',
        51: 'CANCELACIONES',
        52: 'ELIMINACIÓN DE COBERTURAS',
        31: 'Agregar asegurado o dependiente',
        24: 'ALTA DE ASEGURADOS',
        25: 'BAJA DE ASEGURADOS',
        26: 'AJUSTE ANUAL A',
        28: 'AJUSTE ANUAL D',
        4: 'PRORROGA DE VIGENCIA'        
        }
    return switcher.get(request, "Otro")
# # DataFrame aleatorio de ejemplo
# data =  pd.DataFrame(np.random.randn(10, 3), columns=list('ABC'))
# masks = [data.A >= 0,
#         (data.A >= -1) & (data.A < 0),
#          data.A < -1
#         ]    
# colors = ["green", "#ffbf00", "#ff3333"]

# styled = data.style.apply(highlight_col, subset=["B"],
#                           bool_masks=masks, color_list=colors
#                           )
# styled.to_excel("foo.xlsx", sheet_name='Hoja1')
# ¨¨¨¨¨¨¨¨¨¨¨¨---
# auditoría método 
def get_poliza_anterior(self,obj):
    anterior = OldPolicies.objects.filter(new_policy = obj)
    if anterior.exists():
        anterior = anterior.last()
        poliza_anterior = anterior.base_policy.poliza_number
    else:
        poliza_anterior = ''
    return poliza_anterior
def get_poliza_siguiente(self,obj):
    posterior = OldPolicies.objects.filter(base_policy = obj)
    if posterior.exists():
        posterior = posterior.last()
        try:
            poliza_siguiente = posterior.new_policy.poliza_number
        except:
            poliza_siguiente = ''
    else:
        poliza_siguiente = ''
    return poliza_siguiente
def get_endosos_poliza(self, obj):
    endosos = Endorsement.objects.filter(policy = obj).exclude(status=0)
    serializer = EndorsementInfoExcelHyperSerializer(instance = endosos, context = {'request':self.context.get("request")}, many=True)
    return serializer.data
def get_auditoria(self, obj):
    suma = 0
    valores = {}
    try:
        endosos = Endorsement.objects.filter(policy__id = obj.id).exclude(status=0)
        if endosos:
            png_sum = endosos.aggregate(Sum('p_neta'))
            valores['pnetasuma']=Decimal(png_sum['p_neta__sum'] if 'p_neta__sum' in png_sum else 0)+Decimal(obj.p_neta if obj.p_neta else 0)
            ptg_sum = endosos.aggregate(Sum('p_total'))
            valores['ptotalsuma']=Decimal(ptg_sum['p_total__sum'] if 'p_total__sum' in ptg_sum else 0)+Decimal(obj.p_total if obj.p_total else 0)
            rpfg_sum = endosos.aggregate(Sum('rpf'))
            valores['rpfsuma']=Decimal(rpfg_sum['rpf__sum'] if 'rpf__sum' in rpfg_sum else 0)+Decimal(obj.rpf if obj.rpf else 0)
            ivag_sum = endosos.aggregate(Sum('iva'))
            valores['ivasuma']=Decimal(ivag_sum['iva__sum'] if 'iva__sum' in ivag_sum else 0)+Decimal(obj.iva if obj.iva else 0)
            derechog_sum = endosos.aggregate(Sum('derecho'))
            valores['derechosuma']=Decimal(derechog_sum['derecho__sum'] if 'derecho__sum' in derechog_sum else 0)+Decimal(obj.derecho if obj.derecho else 0)
            comisiong_sum = endosos.aggregate(Sum('comision'))
            valores['comisionsuma']=Decimal(comisiong_sum['comision__sum'] if 'comision__sum' in comisiong_sum else 0)+Decimal(obj.comision if obj.comision else 0)
        
        else:
            valores['pnetasuma']= obj.p_neta
            valores['ptotalsuma']= obj.p_total
            valores['rpfsuma']= obj.rpf
            valores['ivasuma']= obj.iva
            valores['derechosuma']= obj.derecho
            valores['comisionsuma']= obj.comision
        # recibos = Recibos.objects.filter(poliza__id = obj.id, isActive=True, isCopy=False, receipt_type__in=[1,2,3]).exclude(status=0).exclude(receipt_type=1)
        recibos = Recibos.objects.filter(poliza__id = obj.id, isActive=True, isCopy=False, receipt_type__in=[1,2,3]).exclude(status=0)
        pagRecibos = Recibos.objects.filter(poliza__id = obj.id, isActive=True, isCopy=False, 
            receipt_type__in=[1,2,3], status=1).exclude(status=0)
        pendRecibos = Recibos.objects.filter(poliza__id = obj.id, isActive=True, isCopy=False, 
            receipt_type__in=[1,2,3], status__in=[4,3]).exclude(status=0)
        cancRecibos = Recibos.objects.filter(poliza__id = obj.id, isActive=True, isCopy=False, 
            receipt_type__in=[1,2,3], status__in=[2]).exclude(status=0)
        today = datetime.today()
        recibos_vencidos = Recibos.objects.filter(fecha_inicio__lte = today,poliza__id = obj.id, isActive=True, isCopy=False, 
            receipt_type__in=[1,2,3], status__in=[3,4]).exclude(status=0)
        valores['vencidos'] = len(recibos_vencidos)
        balComRecPol = Recibos.objects.filter(poliza__id = obj.id, isActive=True, isCopy=False, 
            receipt_type__in=[1]).exclude(status__in=[0,2])
        if recibos:
            rpng_sum = recibos.aggregate(Sum('prima_neta'))
            valores['r_pnetasuma']=Decimal(rpng_sum['prima_neta__sum'] if 'prima_neta__sum' in rpng_sum else 0)
            rptg_sum = recibos.aggregate(Sum('prima_total'))
            valores['r_ptotalsuma']=Decimal(rptg_sum['prima_total__sum'] if 'prima_total__sum' in rptg_sum else 0)
            rpfg_sum = recibos.aggregate(Sum('rpf'))
            valores['r_rpfsuma']=Decimal(rpfg_sum['rpf__sum'] if 'rpf__sum' in rpfg_sum else 0)
            ivag_sum = recibos.aggregate(Sum('iva'))
            valores['r_ivasuma']=Decimal(ivag_sum['iva__sum'] if 'iva__sum' in ivag_sum else 0)
            derechog_sum = recibos.aggregate(Sum('derecho'))
            valores['r_derechosuma']=Decimal(derechog_sum['derecho__sum'] if 'derecho__sum' in derechog_sum else 0)
            comisiong_sum = recibos.aggregate(Sum('comision'))
            valores['r_comisionsuma']=Decimal(comisiong_sum['comision__sum'] if 'comision__sum' in comisiong_sum else 0)
            # total pagado    
            if pagRecibos:            
                tpagado = pagRecibos.aggregate(Sum('prima_total'))
                valores['total_pagado']=Decimal(tpagado['prima_total__sum'] if 'prima_total__sum' in tpagado else 0)
            else:
                valores['total_pagado']=0
            # total pendiente     
            if pendRecibos:           
                tpendiente = pendRecibos.aggregate(Sum('prima_total'))
                valores['total_pendiente']=Decimal(tpendiente['prima_total__sum'] if 'prima_total__sum' in tpendiente else 0)
            else:
                valores['total_pendiente']=0
            # total cancelado    
            if cancRecibos:            
                tcancelado = cancRecibos.aggregate(Sum('prima_total'))
                valores['total_cancelado']=Decimal(tcancelado['prima_total__sum'] if 'prima_total__sum' in tcancelado else 0)
            else:
                valores['total_cancelado']=0
            if balComRecPol:
                balcom = balComRecPol.aggregate(Sum('comision'))
                valores['balance_comision']= obj.comision - Decimal(balcom['comision__sum'] if 'comision__sum' in balcom else 0)
            else:
                valores['balance_comision']=0
        else:
            valores['r_pnetasuma']= obj.p_neta
            valores['r_ptotalsuma']= obj.p_total
            valores['r_rpfsuma']= obj.rpf
            valores['r_ivasuma']= obj.iva
            valores['r_derechosuma']= obj.derecho
            valores['r_comisionsuma']= obj.comision
            valores['total_pagado']=0
            valores['total_pendiente']=0
            valores['total_cancelado']=0
            valores['balance_comision']=0
        return valores
    except Exception as png:
        print('error suma primas neta global',png)
        return 0
# fin auditoría método
def checkStatusPolicy(request):
    switcher = {
        1: "En trámite",
        2: "OT Cancelada",
        4: "Precancelada",
        10: "Por iniciar",
        11: "Cancelada",
        12: "Renovada",
        13: "Vencida",
        14: "Vigente",
        15: "No Renovada",
        16: "Siniestrada",
    }
    return switcher.get(request, "Sin estatus")

def checkCurrency(request):
    switcher = {
        1: "Pesos",
        2: "Dólares",
        3: "UDI",
        4: "Euro",
    }
    return switcher.get(request, "Pesos")

def checkDocumentType(request):
    switcher = {
        1: "Póliza",
        2: "Endoso",
        3: "Póliza de Grupo",
        4: "SubGrupo",
        5: "Categoría",
        6: "Certificado",
        7: "Fianza",
        8: "Fianza Colectiva",
        10: "Fianza Certificado",
        11: "Colectividad",
        12: "Póliza de Colectividad",
    }
    return switcher.get(request, "No aplica")

def checkPayForm(request):
    switcher = {
        1: "Mensual",
        2: "Bimestral",
        3: "Trimestral",
        4: "Cuatrimestral",
        5: "Contado",
        6: "Semestral",
        12: "Anual",
        7:'Semanal',
        14:'Catorcenal',
        24: 'Quincenal',
        15: 'Quincenal',
    }
    return switcher.get(request, "Anual")


def checkRenewable(request):
    switcher = {
        1: "Renovable",
        2: "No Renovable",       
    }
    return switcher.get(request, "NA")

def checkStatusEndoso(request):
    switcher = {
        1: "Pendiente",
        2: "Registrado",
        3: "Rechazado",
        4: "Cancelado",
        5: "En trámite",
        0: "Desactivado",
    }
    return switcher.get(request, "Sin estatus")
def checkStatusSin(request):
    switcher = {        
      3:'Completada/Procedente',
      5:'Rechazada/ No Procedente',
      6:'En espera/ Solicitud de Información',
      1: 'Pendiente', 
      2:  'En Trámite', 
      4: 'Cancelada', 
      7: 'Reproceso', 
      8: 'Inconformidad'
    }
    return switcher.get(request, "Sin estatus")
def checkProc (request):
    switcher = {
        1: "Residencia",
        2: "Turista",
        3: "Legalizado",
        4: "Fronterizo",
    }
    return switcher.get(request, "Otro")

def checkRazonGMM(request):
    switcher = {
        1: "Accidente",
        2: "Enfermedad",
        3: "Parto",
    }
    return switcher.get(request, "No Aplica")

def checkRazonVida(request):
    switcher = {
        1: "Natural",
        2: "Accidental",
        3: "Pérdidas orgánicas",
        4: "Invalidez",
    }
    return switcher.get(request, "No Aplica")

def checkRazonAutos(request):
    switcher = {
        1: "Colisión",
        2: "Incendio",
        3: "Vuelco",
        4: "Robo total",
        5: "Robo parcial",
        6: "Cristales",
        7: "Inundaciones",
        8: "Volcadura",
        9: "Servicio de grúa",
        10: "Pérdida Total",
        11: "Vandalismo",
        12: "Alcance y proyección",
        13: "Daños",
        14: "Fenomenos Naturales",
        15: "Colisión (Afectados)",
        16: "Daños en neumatico",
        17: "Asistencia Vial",
        18: "Atropello",
        19: "Trámites administrativos",
        20: "Pérdida Parcial"
    }
    return switcher.get(request, "No Aplica")

def checkAutosSub1(request):
    switcher = {
        1:'Conclusión de ajuste o peticiones en general',
        2:'Legal',
        3:'Valuación',
        4:'Reparación',
        5:'Daño material',
        6:'Robo',
        7:'Robo localizado',
        9:'Reembolso de gastos médicos',
        10:'Reembolso de gastos funerarios',
        11:'Reembolso de grúa',
        12:'Reembolso de grúa por asistencia',
        13:'Reembolso de cristal',
        14:'Reembolso de deducible o daños a terceros',
        15:'Pago de daños',
        16:'Carta PT y montos',
        17:'Carta rechazo',
        18:'Factura por deducible'
    }
    return switcher.get(request, "")

def checkRazonDanios(request):
    switcher = {
        1: "Accidente",
        2: "Enfermedad",
        3: "Parto",
    }
    return switcher.get(request, "No Aplica")
    
# comit para verificar cambios
