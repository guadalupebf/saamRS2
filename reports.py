from core.checks import *
from core.models import Comments, ReferenciadoresInvolved
from core.utils import get_antiguedad
from contactos.models import ContactInfo
from contratantes.models import GroupingLevel
from forms.models import AutomobilesDamages, Damages, Life, AccidentsDiseases
from recibos.aux import tpfs
import pandas as pd
from core.utils import upload_to_s3
import os
from recibos.group_by import *
from recibos.utils import create_excel_polizas, create_excel


def crea_reporte_agrupado_1p(rows_pesos, rows_dolares, rows_pesosf, rows_dolaresf,rows_udi,rows_udif, post, request_id, redis_client,polizas):
    row_num = 11
    valueCom = post.get('valueCom') if 'valueCom' in post else True
    org = post.get('org')

    since = post.get('since').replace('/', '_')
    until = post.get('until').replace('/', '_')

    agrupacion = []
    prima = []
    total = []
    comision = []
    moneda = []
    registrosA = []
    com_conciliada = []
    dolares = 0
    udi = 0
    p = 0
    c_pesos = 0
    cc_pesos = 0
    c_dolares = 0
    cc_dolares = 0
    c_udi = 0
    cc_udi = 0
    neta_pesos = 0
    neta_dolares = 0
    neta_udi = 0

    for row in rows_pesos:
        if row[0] != None:
            row_num += 1
            c_pesos = c_pesos + row[3]
            neta_pesos = neta_pesos + row[1]
            if row[2]:
                p = p + row[2]
            ag = ((((((row[0]).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')    
            prn = str(row[1])
            prn =(((((prn).replace('[','')).replace(']','')).replace("Decimal(",'')).replace(")",'')).replace("'",'')
            tot = row[2]
            com = row[3]
            mon = 'Pesos'
            regs = row[5]
            agrupacion.append(ag)
            prima.append(prn)
            total.append(tot)
            if valueCom == True or str(valueCom) == 'True':
                comision.append(com)
            else:
                comision.append(0)
            moneda.append(mon)
            registrosA.append(regs)
    # Pesos 2
    for row_1 in rows_pesosf:
        if row_1[0] != None:
            row_num += 1
            c_pesos = c_pesos + row_1[3]
            neta_pesos = neta_pesos + row_1[1]
            if row_1[2]:
                p = p + row_1[2]                    
            ag = ((((((row_1[0]).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')   
            prn = row_1[1]
            tot = row_1[2]
            com = row_1[3]
            mon = 'Pesos'
            regs = row_1[5]
            agrupacion.append(ag)
            prima.append(prn)
            total.append(tot)
            if valueCom == True or str(valueCom) == 'True':
                comision.append(com)
            else:
                comision.append(0)
            moneda.append(mon)
            registrosA.append(regs)
    # dolares 1
    for row_2 in rows_dolares:
        if row_2[0] != None:
            row_num += 1
            c_dolares = c_dolares + row_2[3]
            neta_dolares = neta_dolares + row_2[1]
            if row_2[2]:
                dolares = dolares + row_2[2]
            ag = ((((((row_2[0]).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')   
            prn = row_2[1]
            tot = row_2[2]
            com = row_2[3]
            mon = 'Dólares'
            regs = row_2[5]
            agrupacion.append(ag)
            prima.append(prn)
            total.append(tot)
            comision.append(com)
            moneda.append(mon)
            registrosA.append(regs)
    # dolares 2               
    for row_2f in rows_dolaresf:
        if row_2f[0] != None:
            row_num += 1
            c_dolares = c_dolares + row_2f[3]
            neta_dolares = neta_dolares + row_2f[1]
            if row_2f[2]:
                dolares = dolares + row_2f[2]
            ag = ((((((row_2f[0]).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')   
            prn = row_2f[1]
            tot = row_2f[2]
            com = row_2f[3]
            mon = 'Dólares'
            regs = row_2f[5]
            agrupacion.append(ag)
            prima.append(prn)
            total.append(tot)
            comision.append(com)
            moneda.append(mon)
            registrosA.append(regs)
    # udi 1
    for row_2 in rows_udi:
        if row_2[0] != None:
            row_num += 1
            c_udi = c_udi + row_2[3]
            neta_udi = neta_udi + row_2[1]
            if row_2[2]:
                udi = udi + row_2[2]
            ag = ((((((row_2[0]).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')   
            prn = row_2[1]
            tot = row_2[2]
            com = row_2[3]
            mon = 'UDI'
            regs = row_2[5]
            agrupacion.append(ag)
            prima.append(prn)
            total.append(tot)
            comision.append(com)
            moneda.append(mon)
            registrosA.append(regs)
    # udi 2               
    for row_2f in rows_udif:
        if row_2f[0] != None:
            row_num += 1
            c_udi = c_udi + row_2f[3]
            neta_udi = neta_udi + row_2f[1]
            if row_2f[2]:
                udi = udi + row_2f[2]
            ag = ((((((row_2f[0]).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')   
            prn = row_2f[1]
            tot = row_2f[2]
            com = row_2f[3]
            mon = 'UDI'
            regs = row_2f[5]
            agrupacion.append(ag)
            prima.append(prn)
            total.append(tot)
            comision.append(com)
            moneda.append(mon)
            registrosA.append(regs)
    
    df = pd.DataFrame({
        "Agrupación": agrupacion,
        "Prima": prima,
        "Total": total,
        "Comisión":   comision,
        'Moneda'   : moneda,
    })

    return create_excel_polizas(post, request_id, df, redis_client)

def crea_reporte_agrupado_2p(rows_pesos, rows_dolares, rows_pesosf, rows_dolaresf,rows_udi,rows_udif, post, request_id, redis_client,polizas):
    row_num = 11
    valueCom = post.get('valueCom') if 'valueCom' in post else True
    org = post.get('org')
    since = post.get('since').replace('/', '_')
    until = post.get('until').replace('/', '_')

    tipoContratante = []
    agrupacion = []
    prima = []
    total = []
    comision = []
    moneda = []
    registrosA = []
    dolares = 0
    udi = 0
    p = 0
    c_pesos = 0
    cc_pesos = 0
    c_dolares = 0
    c_udi = 0
    cc_dolares = 0
    cc_udi = 0
    neta_pesos = 0
    neta_dolares = 0
    neta_udi = 0
    for row in rows_pesos:
        if row[0] != None:                    # -------------------
            row_num += 1
            c_pesos = c_pesos + row[3]
            neta_pesos = neta_pesos + row[1]
            if row[2]:
                p = p + row[2]
            ag = ((((((row[0]).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'') 
            tipoc = 'Naturales'   
            prn = row[1]
            tot = row[2]
            com = row[3]
            mon = 'Pesos'
            regs = row[5]
            agrupacion.append(ag)
            tipoContratante.append(tipoc)
            prima.append(prn)
            total.append(tot)
            if valueCom == True or str(valueCom) == 'True':
                comision.append(com)
            else:
                comision.append(0)
            moneda.append(mon)
            registrosA.append(regs)
    # Pesos 2
    for row_1 in rows_pesosf:
        pesos_total_f = 0
        if row_1[0] != None:
            row_num += 1
            c_pesos = c_pesos + row_1[3]
            neta_pesos = neta_pesos + row_1[1]
            if row_1[2]:
                p = p + row_1[2]                    
            ag = ((((((row_1[0]).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')   
            tipoc = 'Morales'
            prn = row_1[1]
            tot = row_1[2]
            com = row_1[3]
            mon = 'Pesos'
            regs = row_1[5]
            agrupacion.append(ag)
            tipoContratante.append(tipoc)
            prima.append(prn)
            total.append(tot)
            if valueCom == True or str(valueCom) == 'True':
                comision.append(com)
            else:
                comision.append(0)
            moneda.append(mon)
            registrosA.append(regs)
    # dolares 1
    for row_2 in rows_dolares:
        if row_2[0] != None:
            row_num += 1
            c_dolares = c_dolares + row_2[3]
            neta_dolares = neta_dolares + row_2[1]
            if row_2[2]:
                dolares = dolares + row_2[2]
            ag = ((((((row_2[0]).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')   
            tipoc = 'Naturales'
            prn = row_2[1]
            tot = row_2[2]
            com = row_2[3]
            mon = 'Dolares'
            regs = row_2[5]
            agrupacion.append(ag)
            tipoContratante.append(tipoc)
            prima.append(prn)
            total.append(tot)
            if valueCom == True or str(valueCom) == 'True':
                comision.append(com)
            else:
                comision.append(0)
            moneda.append(mon)
            registrosA.append(regs)
    # dolares 2          
    for row_2f in rows_dolaresf:
            if row_2f[0] != None:
                row_num += 1
                c_dolares = c_dolares + row_2f[3]
                neta_dolares = neta_dolares + row_2f[1]
                if row_2f[2]:
                    dolares = dolares + row_2f[2]
                ag = ((((((row_2f[0]).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')   
                tipoc = 'Morales'
                prn = row_2f[1]
                tot = row_2f[2]
                com = row_2f[3]
                mon = 'Dolares'
                regs = row_2f[5]
                agrupacion.append(ag)
                tipoContratante.append(tipoc)
                prima.append(prn)
                total.append(tot)
                if valueCom == True or str(valueCom) == 'True':
                    comision.append(com)
                else:
                    comision.append(0)
                moneda.append(mon)
                registrosA.append(regs)
    # udi 1
    for row_2 in rows_udi:
        if row_2[0] != None:
            row_num += 1
            c_udi = c_udi + row_2[3]
            neta_udi = neta_udi + row_2[1]
            if row_2[2]:
                udi = udi + row_2[2]
            ag = ((((((row_2[0]).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')   
            tipoc = 'Naturales'
            prn = row_2[1]
            tot = row_2[2]
            com = row_2[3]
            mon = 'UDi'
            regs = row_2[5]
            agrupacion.append(ag)
            tipoContratante.append(tipoc)
            prima.append(prn)
            total.append(tot)
            if valueCom == True or str(valueCom) == 'True':
                comision.append(com)
            else:
                comision.append(0)
            moneda.append(mon)
            registrosA.append(regs)
    # udi 2               
    for row_2f in rows_udif:
            if row_2f[0] != None:
                row_num += 1
                c_udi = c_udi + row_2f[3]
                neta_udi = neta_udi + row_2f[1]
                if row_2f[2]:
                    udi = udi + row_2f[2]
                ag = ((((((row_2f[0]).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')   
                tipoc = 'Morales'
                prn = row_2f[1]
                tot = row_2f[2]
                com = row_2f[3]
                mon = 'UDI'
                regs = row_2f[5]
                agrupacion.append(ag)
                tipoContratante.append(tipoc)
                prima.append(prn)
                total.append(tot)
                if valueCom == True or str(valueCom) == 'True':
                    comision.append(com)
                else:
                    comision.append(0)
                moneda.append(mon)
                registrosA.append(regs)
    df = pd.DataFrame({
        "Agrupación": agrupacion,
        "Prima": prima,
        "Total": total,
        "Comisión":   comision,
        'Moneda'   : moneda,
    })

    return create_excel_polizas(post, request_id, df, redis_client)

def crea_reporte_agrupado_3p(rows_pesos, rows_dolares,rows_udi, post, request_id, redis_client):
    row_num = 11
    valueCom = post.get('valueCom') if 'valueCom' in post else True
    org = post.get('org')
    since = post.get('since').replace('/', '_')
    until = post.get('until').replace('/', '_')

    tipoContratante = []
    agrupacion = []
    prima = []
    total = []
    comision = []
    moneda = []
    registrosA = []
    com_conciliada = []
    dolares = 0
    udi = 0
    p = 0
    c_pesos = 0
    cc_pesos = 0
    c_dolares = 0
    c_udi = 0
    cc_dolares = 0
    cc_udi = 0
    neta_pesos = 0
    neta_dolares = 0
    neta_udi= 0
    for row in rows_pesos:
        if row[0] != None:
            row_num += 1
            c_pesos = c_pesos + row[3]
            neta_pesos = neta_pesos + row[1]
            if row[2]:
                p = p + row[2]
            ag = row[0] + ' ' + str(row[6])
            tipoc = 'Pólizas'   
            prn = row[1]
            tot = row[2]
            com = row[3]
            mon = 'Pesos'
            regs = row[5]
            agrupacion.append(ag)
            tipoContratante.append(tipoc)
            prima.append(prn)
            total.append(tot)
            if valueCom == True or str(valueCom) == 'True':
                comision.append(com)
            else:
                comision.append(0)
            moneda.append(mon)
            registrosA.append(regs)

    # dolares 1
    for row_2 in rows_dolares:
        if row_2[0] != None:
            row_num += 1
            c_dolares = c_dolares + row_2[3]
            neta_dolares = neta_dolares + row_2[1]
            if row_2[2]:
                dolares = dolares + row_2[2]
            ag = row_2[0] +' '+str(row_2[6])
            tipoc = 'Pólizas'
            prn = row_2[1]
            tot = row_2[2]
            com = row_2[3]
            mon = 'Dolares'
            regs = row_2[5]
            agrupacion.append(ag)
            tipoContratante.append(tipoc)
            prima.append(prn)
            total.append(tot)
            if valueCom == True or str(valueCom) == 'True':
                comision.append(com)
            else:
                comision.append(0)
            moneda.append(mon)
            registrosA.append(regs)
    for row_2 in rows_udi:
        if row_2[0] != None:
            row_num += 1
            c_udi = c_udi + row_2[3]
            neta_udi = neta_udi + row_2[1]
            if row_2[2]:
                udi = udi + row_2[2]
            ag = row_2[0] +' '+str(row_2[6])
            tipoc = 'Pólizas'
            prn = row_2[1]
            tot = row_2[2]
            com = row_2[3]
            mon = 'UDI'
            regs = row_2[5]
            agrupacion.append(ag)
            tipoContratante.append(tipoc)
            prima.append(prn)
            total.append(tot)
            if valueCom == True or str(valueCom) == 'True':
                comision.append(com)
            else:
                comision.append(0)
            moneda.append(mon)
            registrosA.append(regs)

    df = pd.DataFrame({
        "Agrupación": agrupacion,
        "Prima": prima,
        "Total": total,
        "Comisión":   comision,
        'Moneda'   : moneda,
    })
    return create_excel_polizas(post, request_id, df, redis_client)

def crea_reporte_agrupado_4p(rows_pesos, rows_dolares, rows_pesosf, rows_dolaresf,rows_udi,rows_udif, post, request_id, redis_client,polizas):
    row_num = 11
    valueCom = post.get('valueCom') if 'valueCom' in post else True
    org = post.get('org')
    since = post.get('since').replace('/', '_')
    until = post.get('until').replace('/', '_')

    agrupacion = []
    prima = []
    total = []
    comision = []
    moneda = []
    registrosA = []
    com_conciliada = []
    dolares = 0
    udi = 0
    p = 0
    c_pesos = 0
    cc_pesos = 0
    c_dolares = 0
    c_udi = 0
    cc_dolares = 0
    cc_udi = 0
    neta_pesos = 0
    neta_dolares = 0
    neta_udi = 0
    for row in rows_pesos:
        if row[0] != None:
            row_num += 1
            c_pesos = c_pesos + row[3]
            neta_pesos = neta_pesos + row[1]
            if row[2]:
                p = p + row[2]
            ag = row[0]+ ' ' + str(row[6])  
            prn = row[1]
            tot = row[2]
            com = row[3]
            mon = 'Pesos'
            regs = row[5]
            agrupacion.append(ag)
            prima.append(prn)
            total.append(tot)
            if valueCom == True or str(valueCom) == 'True':
                comision.append(com)
            else:
                comision.append(0)
            moneda.append(mon)
            registrosA.append(regs)
    # Pesos 2
    for row_1 in rows_pesosf:
        pesos_total_f = 0
        if row_1[0] != None:
            row_num += 1
            c_pesos = c_pesos + row_1[3]
            cc_pesos = cc_pesos + row_1[6]
            neta_pesos = neta_pesos + row_1[1]
            if row_1[2]:
                p = p + row_1[2]       
            ag = row_1[0]+ ' ' + str(row_1[6])  
            prn = row_1[1]
            tot = row_1[2]
            com = row_1[3]
            mon = 'Pesos'
            regs = row_1[5]
            agrupacion.append(ag)
            prima.append(prn)
            total.append(tot)
            if valueCom == True or str(valueCom) == 'True':
                comision.append(com)
            else:
                comision.append(0)
            moneda.append(mon)
            registrosA.append(regs)
    # dolares 1
    for row_2 in rows_dolares:
        if row_2[0] != None:
            row_num += 1
            c_dolares = c_dolares + row_2[3]
            neta_dolares = neta_dolares + row_2[1]
            if row_2[2]:
                dolares = dolares + row_2[2]
            ag = row_2[0]+ ' ' + str(row_2[6]) 
            prn = row_2[1]
            tot = row_2[2]
            com = row_2[3]
            mon = 'Dolares'
            regs = row_2[5]
            agrupacion.append(ag)
            prima.append(prn)
            total.append(tot)
            if valueCom == True or str(valueCom) == 'True':
                comision.append(com)
            else:
                comision.append(0)
            moneda.append(mon)
            registrosA.append(regs)
    # dolares 2               
    for row_2f in rows_dolaresf:
            if row_2f[0] != None:
                row_num += 1
                c_dolares = c_dolares + row_2f[3]
                neta_dolares = neta_dolares + row_2f[1]
                if row_2f[2]:
                    dolares = dolares + row_2f[2]
                ag = row_2f[0]+ ' ' + str(row_2f[6])  
                prn = row_2f[1]
                tot = row_2f[2]
                com = row_2f[3]
                mon = 'Dolares'
                regs = row_2f[5]
                agrupacion.append(ag)
                prima.append(prn)
                total.append(tot)
                if valueCom == True or str(valueCom) == 'True':
                    comision.append(com)
                else:
                    comision.append(0)
                moneda.append(mon)
                registrosA.append(regs)

    # udi 1
    for row_2 in rows_udi:
        if row_2[0] != None:
            row_num += 1
            c_udi = c_udi + row_2[3]
            neta_udi = neta_udi + row_2[1]
            if row_2[2]:
                udi = udi + row_2[2]
            ag = row_2[0]+ ' ' + str(row_2[6]) 
            prn = row_2[1]
            tot = row_2[2]
            com = row_2[3]
            mon = 'UDI'
            regs = row_2[5]
            agrupacion.append(ag)
            prima.append(prn)
            total.append(tot)
            if valueCom == True or str(valueCom) == 'True':
                comision.append(com)
            else:
                comision.append(0)
            moneda.append(mon)
            registrosA.append(regs)
    # udi 2               
    for row_2f in rows_udif:
            if row_2f[0] != None:
                row_num += 1
                c_udi = c_udi + row_2f[3]
                neta_udi = neta_udi + row_2f[1]
                if row_2f[2]:
                    udi = udi + row_2f[2]
                ag = row_2f[0]+ ' ' + str(row_2f[6])  
                prn = row_2f[1]
                tot = row_2f[2]
                com = row_2f[3]
                mon = 'UDI'
                regs = row_2f[5]
                agrupacion.append(ag)
                prima.append(prn)
                total.append(tot)
                if valueCom == True or str(valueCom) == 'True':
                    comision.append(com)
                else:
                    comision.append(0)
                moneda.append(mon)
                registrosA.append(regs)
    df = pd.DataFrame({
        "Agrupación": agrupacion,
        "Prima": prima,
        "Total": total,
        "Comisión":   comision,
        'Moneda'   : moneda,

    })
    return create_excel_polizas(post, request_id, df, redis_client)
  

def crea_reporte_agrupado_5p(rows_pesos, rows_dolares,rows_udi, post, request_id, redis_client):
    row_num = 11
    valueCom = post.get('valueCom') if 'valueCom' in post else True
    org = post.get('org')

    since = post.get('since').replace('/', '_')
    until = post.get('until').replace('/', '_')

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
            ag = checkStatusPolicy(int(row[0]))    
            prn = str(row[1])
            prn =(((((prn).replace('[','')).replace(']','')).replace("Decimal(",'')).replace(")",'')).replace("'",'')
            tot = row[2]
            com = row[3]
            mon = 'Pesos'
            regs = row[5]
            agrupacion.append(ag)
            prima.append(prn)
            total.append(tot)
            if valueCom == True or str(valueCom) == 'True':
                comision.append(com)
            else:
                comision.append(0)
            moneda.append(mon)
            registrosA.append(regs)
    # dolares 1
    for row_2 in rows_dolares:
        if row_2[0] != None:
            row_num += 1
            c_dolares = c_dolares + row_2[3]
            neta_dolares = neta_dolares + row_2[1]
            if row_2[2]:
                dolares = dolares + row_2[2]
            ag = ((((((row_2[0]).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')   
            prn = row_2[1]
            tot = row_2[2]
            com = row_2[3]
            mon = 'Dolares'
            regs = row_2[5]
            agrupacion.append(ag)
            prima.append(prn)
            total.append(tot)
            comision.append(com)
            moneda.append(mon)
            registrosA.append(regs)    
    # udi 1
    for row_2 in rows_udi:
        if row_2[0] != None:
            row_num += 1
            c_udi = c_udi + row_2[3]
            neta_udi = neta_udi + row_2[1]
            if row_2[2]:
                udi = udi + row_2[2]
            ag = ((((((row_2[0]).replace('[','')).replace(']',''))).replace(",",'')).replace(".",'')).replace("'",'')   
            prn = row_2[1]
            tot = row_2[2]
            com = row_2[3]
            mon = 'UDI'
            regs = row_2[5]
            agrupacion.append(ag)
            prima.append(prn)
            total.append(tot)
            comision.append(com)
            moneda.append(mon)
            registrosA.append(regs)   
    df = pd.DataFrame({
        "Agrupación": agrupacion,
        "Prima": prima,
        "Total": total,
        "Comisión":   comision,
        'Moneda'   : moneda,
    })

    return create_excel_polizas(post, request_id, df, redis_client)
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