from contratantes.models import *
from aseguradoras.models import *
from ramos.models import *
from django.contrib.auth.models import User
from claves.models import *
from polizas.models import *
from django.db.models import Q
from functools import reduce
import operator

def filter_polizas(data):
    org = data['org']

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
    # --------****
    # oriegn 1: nueva; 2 renovación; 0 ambas
    try:
        origen = data['origin']
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
            fp = [12,24,6,5,4,3,2,1]

    # Filtro de status
    if int(status) > 0:
            st = [int(status)]
    else:
            st = [1,2,4,10,11,12,13,14,15]
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
        subg = Group.objects.filter(parent__id = int(group), type_group = 2, org=  org).values_list('pk', flat=True)
        subsubg = Group.objects.filter(parent__id__in = subg, type_group = 3, org=  org).values_list('pk', flat=True)
        allgrupos = list(subg) + list(subsubg)
        allgrupos.append(grupos1.id)
        grupos = Group.objects.filter(pk__in = allgrupos, org = org)
    else:
        grupos = Group.objects.filter(org = org)

    if int(subgrupo) > 0:
        subg = Group.objects.get(pk = int(subgrupo), org=  org)
        subsubg = Group.objects.filter(parent__id = int(subgrupo), type_group = 3, org=  org).values_list('pk', flat=True)
        allgrupos =list(subsubg)
        allgrupos.append(subg.id)
        grupos = Group.objects.filter(pk__in = allgrupos, org = org)

    if int(subsubgrupo) > 0:
        grupos = Group.objects.get(pk = int(subsubgrupo), org=  org)

    # -----GRUPOS-----
    if int(nivelagrupacion) > 0:
        levels1 = GroupingLevel.objects.get(pk = int(nivelagrupacion))
        levels2 = GroupingLevel.objects.filter(parent__id = int(nivelagrupacion), type_grouping = 2, org=  org).values_list('pk', flat=True)
        levels3 = GroupingLevel.objects.filter(parent__id__in = levels2, type_grouping = 3, org=  org).values_list('pk', flat=True)
        levelgropings = list(levels2) + list(levels3)
        levelgropings.append(levels1.id)
        levelGrouping = GroupingLevel.objects.filter(pk__in = levelgropings, org = org)
    else:
        levelGrouping = GroupingLevel.objects.filter(org = org)

    if int(subnivel) > 0:
        levels2 = GroupingLevel.objects.get(pk = int(subnivel), org=  org)
        levels3 = GroupingLevel.objects.filter(parent__id = int(subnivel), type_grouping = 3, org=  org).values_list('pk', flat=True)
        levelgropings =list(levels3)
        levelgropings.append(levels2.id)
        levelGrouping = GroupingLevel.objects.filter(pk__in = levelgropings, org = org)

    if int(subsubnivel) > 0:
        levelGrouping = GroupingLevel.objects.get(pk = int(subsubnivel), org=  org)

    if int(clasificacion) > 0:
        clasifics = Classification.objects.get(pk = int(clasificacion), org = org)
    else:
        clasifics = Classification.objects.filter(org = org)


    # Filtro de aseguradora
    if (providers_sel):
            providers = list(Provider.objects.filter(pk__in = (providers_sel)).values_list('pk', flat=True))
    else:
            providers = list(Provider.objects.filter(org = org).values_list('pk', flat=True))

    # Filtro de ramo
    if (ramos_sel):
            ramos = list(Ramos.objects.filter(org = org, provider__in = providers,ramo_code__in=ramos_sel).values_list('pk', flat=True))
    else:
            ramos = list(Ramos.objects.filter(org = org, provider__in = providers).values_list('pk', flat=True))

    # Filtro de subramo
    if (subramos_sel):
            subramos = list(SubRamos.objects.filter(org = org, ramo__in = ramos,subramo_code__in= subramos_sel).values_list('pk', flat=True))
    else:
            subramos = list(SubRamos.objects.filter(org = org, ramo__in = ramos).values_list('pk', flat=True))   
    
    # Filtro de usuario
    if (users_sel):
            users = list(User.objects.filter(pk__in = (users_sel)).values_list('pk', flat=True))
    else:
            users = list(User.objects.values_list('pk', flat=True))

    # Filtro de clave
    try:
        clave = int(cve)
        if clave > 0:
            cves = list(Claves.objects.filter(clave__icontains = cve, org = org).values_list('pk', flat=True))
        else:
            cves = list(Claves.objects.filter(org = org).values_list('pk', flat=True))
    except:
        cves = list(Claves.objects.filter(clave__icontains = cve, org = org).values_list('pk', flat=True))
    
    if npolicy:
        polizas = Polizas.objects.filter(status__in = st, 
                                                                     forma_de_pago__in = fp, 
                                                                     org=org,
                                                                     ramo__in = ramos, 
                                                                     subramo__in = subramos, 
                                                                     aseguradora__in = providers,
                                                                     clave__in = cves,
                                                                     owner__in = users,
                                                                     poliza_number__icontains = npolicy)
    elif identif:
        polizas = Polizas.objects.filter(status__in = st, 
                                                                     forma_de_pago__in = fp, 
                                                                     org=org,
                                                                     ramo__in = ramos, 
                                                                     subramo__in = subramos, 
                                                                     aseguradora__in = providers,
                                                                     clave__in = cves,
                                                                     owner__in = users,
                                                                     identifier__icontains = identif)
    else:
        polizas = Polizas.objects.filter(status__in = st, 
                                                                     forma_de_pago__in = fp, 
                                                                     org=org,
                                                                     ramo__in = ramos, 
                                                                     subramo__in = subramos, 
                                                                     aseguradora__in = providers,
                                                                     clave__in = cves,
                                                                     owner__in = users)

    
    # Filtro de contratante
    if int(contratante) > 0 :
        if type_person == 'natural':
            contratanten = list( NaturalPerson.objects.filter(pk = int(contratante), group = grupos).values_list('pk', flat = True))
            polizas = polizas.filter(natural__in = contratanten)
        
        else:
            contratantej = list(Juridical.objects.filter(pk = int(contratante), group = grupos).values_list('pk', flat = True) )
            polizas = polizas.filter(juridical__in = contratantej)

    
    else:
        contratanten = list(NaturalPerson.objects.filter(group__in = grupos).values_list('pk', flat = True))
        contratantej = list(Juridical.objects.filter(group__in = grupos).values_list('pk', flat = True) )

        if len(contratanten) and len(contratantej):
            polizas = polizas.filter(Q(natural__in = contratanten) | Q(juridical__in = contratantej))

    # ----------------------******************************---------------------------------------------------
    if int(group) > 0 :
        polizas = polizas.filter(Q(natural__group = grupos) | Q(juridical__group = grupos))
    if int(nivelagrupacion) > 0 :
        polizas = polizas.filter(Q(natural__grouping_level = levelGrouping) | Q(juridical__grouping_level = levelGrouping))
    # -----------------------------***********************************************-----------------------------------   
    if int(clasificacion) > 0 :
        polizas = polizas.filter(Q(natural__classification = clasifics) | Q(juridical__classification = clasifics))
    if int(bsLine) > 0:
        if int(bsLine) == 3:
            polizas = polizas.filter(business_line = 0) 
        else:
            polizas = polizas.filter(business_line = bsLine)
    # -----------------------------***********************************************-----------------------------------
    # Filtro de renovadas
    if add_renovadas == 1:
        polizas = polizas.filter(renewed = True , renewed_status = 1)
    elif add_renovadas == 2:
        polizas = polizas.filter(renewed = False , renewed_status = 0)
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

    if ot_rep == 1:
        polizas = polizas.filter(reduce(operator.and_, date_filters), document_type__in = list([1,3]) ,org = org, status__in = [1,2])
    elif ot_rep == 2:
        polizas = polizas.filter(reduce(operator.and_, date_filters), document_type__in = list([1,3]) ,org = org)
    elif ot_rep == 3:
        polizas = polizas.filter(reduce(operator.and_, date_filters), document_type__in = list([1,3]) ,org = org).exclude(status__in = [1,2])

    vendedor = data['vendedor']
    if vendedor != 0:
        vendors = list(Vendedor.objects.filter(pk = int(vendedor)).values_list('pk', flat=True))
        polizas = polizas.filter((Q(ref_policy__referenciador__id__in = [vendedor]) | (Q(vendor__id__in = vendors))))

    else:
        polizas = polizas
    if sucursal != 0:
        polizas = polizas.filter(sucursal__id = sucursal)
    # Try
    org = org
    polizasK = polizas.values_list('pk',flat = True)
    old_policies = OldPolicies.objects.filter(org=org)
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
        # print('polizasNewAdd1 ', polizasNewAdd1 )
        # 
        polizasR_ = Polizas.objects.filter(pk__in=polizasEvaluateN)
        polizasR_ = polizasR_.filter(pk__in = polizasK)
        polizasNewAdd2 = polizasR_

    return polizas, polizasNewAdd1, polizasNewAdd2