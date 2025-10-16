from django.urls import path
from .views import *
# from .pyexcelerate_prueba import *
from rest_framework import routers

router = routers.SimpleRouter()

app_name = 'polizas'

urlpatterns = [
	path('polizas/', Polizas, name='polizas'),
	path('certificados/', Certificados, name='certificados'),
	path('reporte-polizas-asincrono/', ReportePolizas, name='certificados'),
	path('reporte_renovacionesAIA/',RenovacionesReportAIA, name='RenovacionesReportAIA'),
	path('reporte_polizasAIA/',PolizasReportAIA, name='PolizasReportAIA'),
	path('reporte_fianzasAIA/',FianzasReportAIA, name='FianzasReportAIA'),
	path('reporte_endososAIA/',EndososReportAIA, name='EndososReportAIA'),
	path('reporte_siniestrosAIA/',SiniestrosReportAIA, name='SiniestrosReportAIA'),
	path('carga_massive_certificates/',CheckCertificatesCargaMasiva, name='CheckCertificatesCargaMasiva'),
	path('carga_massive_infocontributoria/',CheckInfoContributoriaCargaMasiva, name='CheckInfoContributoriaCargaMasiva'),
	path('reporte-polizascontrib-asincrono/', ReportePolizasContrib, name='polizascontributorias'),
	path('reporte-polizas-asincrono_ancora/', ReportePolizasAncora, name='polizasreporteancora'),
	path('certificados-ancora/', CertificadosAncora, name='certificadosAncora'),
	path('polizascertificados/', CertificadosAncora, name='polizascertificados'),
	# path('polizascertificados/', ReportePolizasCertificados, name='polizascertificados'),#prueba
	path('reporte-auditoria-asincrono/', ReporteAuditoria, name='auditoriapolizas'),
]

urlpatterns += router.urls