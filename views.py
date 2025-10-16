from django.views.decorators.csrf import csrf_exempt
from contratantes.models import *
from ramos.models import *
from claves.models import *
from .tasks import *
import json, time
from django.http import HttpResponse, HttpResponseNotFound
from rest_framework.decorators import api_view, permission_classes
from rest_framework.response import Response
from .utils import *
from core.utils import TokenRevision
# from .pyexcelerate_prueba import *

@csrf_exempt
def Polizas(request):
    data = validate_data(request)
    if isinstance(data, str):
        return HttpResponseNotFound('<p>'+ data +'</p>')

    try:
        count = data['count']
    except Exception as e:
        polizas = filter_polizas(data)[0]
        count = len(polizas)

    es_asincrono = False

    if count > 10:
        es_asincrono = True

    if es_asincrono == True:
#        print('soy asincrono')
        create_report_polizas_task.delay(data, es_asincrono)
        return HttpResponse(json.dumps({'Async': True}), content_type="application/json")
    else:
#        print('soy sincrono')
        r = create_report_polizas_task(data, es_asincrono)
        return r


@api_view(['POST'])
@permission_classes((TokenRevision, ))
def ReportePolizas(request):
    # print(request.data)
    request.query_params._mutable = True
    post = request.POST.copy()
    post['cols'] = post.getlist('cols')
    task_id = reporte_polizas_asincrono.delay(request.POST,post)    
    return HttpResponse(task_id)  
    # return Response(json.dumps(), content_type="application/json")

@api_view(['POST'])
@permission_classes((TokenRevision, ))
def Certificados(request):
    request.query_params._mutable = True
    post = request.POST.copy() # to make it mutable
    post['select_ramo'] = post.getlist('select_ramo')
    post['select_provider'] = post.getlist('select_provider')
    post['select_sramo'] = post.getlist('select_sramo')
    post['select_user'] = post.getlist('select_user')
    certificate_report_task.delay(request.POST,post)
    return Response(json.dumps({'Status': 'Loading'}), content_type="application/json")
# @api_view(['POST'])
# @permission_classes((TokenRevision, ))
# def Certificados(request):
#     reporte_polizas_asincrono.delay(request.POST)
#     return Response(json.dumps({'Status': 'Loading'}), content_type="application/json")


#AIA
@api_view(['POST'])
#@permission_classes((TokenRevision, ))
def RenovacionesReportAIA(request):
    post = request.POST.copy()
    task_id = renovaciones_reportaia_task(post)
    return JsonResponse(task_id)

@api_view(['POST'])
#@permission_classes((TokenRevision, ))
def PolizasReportAIA(request):
    post = request.POST.copy()
    task_id = polizas_reportaia_task(post)
    return JsonResponse(task_id)

@api_view(['POST'])
#@permission_classes((TokenRevision, ))
def FianzasReportAIA(request):
    post = request.POST.copy()
    task_id = fianzas_reportaia_task(post)
    return JsonResponse(task_id)

@api_view(['POST'])
#@permission_classes((TokenRevision, ))
def EndososReportAIA(request):
    request.query_params._mutable = True
    post = request.POST.copy()
    post['status'] = post.getlist('status')
    task_id = endosos_reportaia_task(request.POST,post)
    return JsonResponse(task_id)

@api_view(['POST'])
#@permission_classes((TokenRevision, ))
def SiniestrosReportAIA(request):
    request.query_params._mutable = True
    post = request.POST.copy()
    post['status'] = post.getlist('status')
    task_id = siniestros_reportaia_task(request.POST,post)
    return JsonResponse(task_id)
#cer masiva carga
@api_view(['POST'])
@permission_classes((TokenRevision, ))
def CheckCertificatesCargaMasiva(request):
    request.query_params._mutable = True
    post = request.POST.copy()
    post['certificados'] =post.getlist('certificados')
    certificados = request.POST.get('certificados')
    task_id = certificados_to_evaluate.delay(request.POST,post,certificados)
    return HttpResponse(task_id)  
@api_view(['POST'])
@permission_classes((TokenRevision, ))
def CheckInfoContributoriaCargaMasiva(request):
    request.query_params._mutable = True
    post = request.POST.copy()
    post['polizas'] =post.getlist('polizas')
    polizas = request.POST.get('polizas')
    task_id = infocontributory_to_evaluate.delay(request.POST,post,polizas)
    return HttpResponse(task_id)  
@api_view(['POST'])
@permission_classes((TokenRevision, ))
def ReportePolizasContrib(request):
    # print(request.data)
    request.query_params._mutable = True
    post = request.POST.copy()
    post['cols'] = post.getlist('cols')
    task_id = reporte_polizascontrib_asincrono.delay(request.POST,post)    
    return HttpResponse(task_id)  

@api_view(['POST'])
# @permission_classes((TokenRevision, ))
def ReportePolizasAncora(request):
    # print(request.data)
    request.query_params._mutable = True
    post = request.POST.copy()
    post['cols'] = post.getlist('cols')
    try:
        gettingby = request.POST.get('gettingby')
        print('gettingby',gettingby)
        if gettingby:
            print('por script ancora')
            task_id = reporte_polizas_asincrono(request.POST,post)  
            print('task--data-----------',task_id)
            return JsonResponse(task_id)
        else:
            task_id = reporte_polizas_asincrono.delay(request.POST,post)    
            return HttpResponse(task_id)  
    except Exception as e:
        print('err',e)
        task_id = reporte_polizas_asincrono.delay(request.POST,post)    
        return HttpResponse(task_id)  
    # return Response(json.dumps(), content_type="application/json")
# @api_view(['POST'])
# @permission_classes((TokenRevision, ))
def CertificadosAncora(request):
    # request.query_params._mutable = True
    # post = request.POST.copy() # to make it mutable
    post = request
    post['select_ramo'] = post['select_ramo']
    post['select_provider'] = post['select_provider']
    post['select_sramo'] = post['select_sramo']
    post['select_user'] = post['select_user']
    # certificate_report_task.delay(request.POST,post)    
    task_id = certificate_report_task(request,post)  
    return JsonResponse(task_id)
    # return Response(json.dumps({'Status': 'Loading'}), content_type="application/json")
@api_view(['POST'])
# @permission_classes((TokenRevision, ))
def ReportePolizasCertificados(request):
    # print(request.data)
    request.query_params._mutable = True
    post = request.POST.copy()
    task_id = getPolizasCertificados.delay(request.POST,post)    
    return HttpResponse(task_id)  

@api_view(['POST'])
@permission_classes((TokenRevision, ))
def ReporteAuditoria(request):
    request.query_params._mutable = True
    post = request.POST.copy()
    post['cols'] = post.getlist('cols')
    task_id = reporte_auditoria_asincrono.delay(request.POST,post)    
    return HttpResponse(task_id)  

def ReportePolizasAncoraOK(data):
    # request.query_params._mutable = True
    post =data
    post['cols'] = post['cols']
    try:
        print('por script ancora')
        task_id = reporte_polizas_asincrono(data,post)  
        print('task--data-----------',task_id)
        return JsonResponse(task_id)         
    except Exception as e:
        print('err',e)
        task_id = reporte_polizas_asincrono.delay(data,post)    
        return HttpResponse(task_id)  
