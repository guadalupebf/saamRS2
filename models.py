# -*- coding: utf-8 -*-
from contratantes.models import CelulaContractor, GroupingLevel, Contractor
from core.models import TimeStampedModel
from django.db import models
from core.models import *
from claves.models import Claves
from decimal import Decimal
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.contrib.postgres.fields import ArrayField

#Los status 3, 5, 6, 7, 20 y 21 son para el refactor de status, se actualizarán hasta que se corra fix_polizas_renovacion_status
STATUS = [ (0,'Desactivado'), 
            # Aplica solo para SubGrupo y Categorías)
            (3,'N/A'),
            #Aplica para polizas y fianzas, tanto individual como colectivo
            (1,'En trámite'), (2, 'OT Cancelada.'), (10,'Por iniciar'), (14,'Vigente'), (4, 'Vigente (Precancelada)'),
            (11, 'Cancelada'), (13, 'Vencida'), 
            (12,'Renovada'), (15,'No Renovada'),
            (16,'PreAnulada'), (17,'Anulada'),
            (18,'Activo por Endosos A'),(19,'Inactivo por Endoso D'),
            (20,'Inactivo inicial por Cancelación'),(21,'Inactivo Endoso A por Cancelación'), 
            (22,'Inactivo por Póliza Vencida'),(23,'Inactivo por Póliza Renovada'),(24,'PreAnulada')
            ]

STATUS_EMISION = [ (1,'Documentos recibidos'), (2, 'Documentos pendientes'), (3,'Carga de documentos'),(4,'En autorización'),(5,'Emitida')]
STATUS_COT = [ (0,'Desactivado'), (1,'En trámite'), (2, 'Emitida.'), (3, 'Bateada')]
FORMA_DE_PAGO = [(1, 'Mensual'), (2, 'Bimestral'), (3, 'Trimestral'), (5, 'Contado'), (6, 'Semestral'), 
                (12, 'Anual'), (24, 'Quincenal'),(4, 'Cuatrimestral'),(7, 'Semanal'),(14, 'Catorcenal')]
F_CURRENCY_CHOICES = ((1, 'Pesos'), (2, 'Dólares'), (3, 'UDI'), (4, 'Euro'))
RENEWED_CHOICES = ((1, 'Renovable'), (2, 'No Renovable'))
RECEIPTS_CHOICES = ((1, 'Póliza'), (2, 'Subgrupo'),(3,'Categoría'))
RENEWAL_CHOICES = ((0, 'No existe renovacción posterior'), (2, 'En proceso de renovación'), (1, 'Renovación completa'), (3, 'Error, existen multiples renovaciones'))

ADMINISTRATION_TYPES = ((1, 'Simplificada'), (2, 'Autoadministrada'), (3, 'Detallada'))
DOCUMENT_TYPE = ((1, 'Póliza'), (3, 'Póliza de grupo'), (4, 'SubGrupo'), (5, 'Categoría'), (6, 'Certificado'),(7, 'Fianza'),(8,'Carátula Fianza'),(9,'Categoría Fianza'),(10,'Certificado Fianza'),(11,'Colectividad'),(12,'Póliza de Colectividad'))

TIPOPOLICY_CHOICES = ((1, 'Abierta'),(2, 'Cerrada'))
CONCEPT_CHOICES = ((1, 'Preanulada'),(2, 'Por error de captura'),(3, 'Por reexpedición'),(4, 'Por falta de pago'), (5, 'Otro(Anulación)'), (0, 'No aplica'))
PAYSCHEME_CHOICES = ((1, 'Tradicional'), (2, 'Prima Mínima'))
BUSINESSLINE_CHOICES = ((1, 'Comercial'), (2, 'Personal'),(0,'Otro'))

DOMICILIADA_CHOICES = ( (1, 'No domiciliada'), (2, 'Agente'),(3,'CAC'),(4,'CAT/Domiciliado'),(5,'Nómina'),(6,'CUT') )
MONTH_CHOICES = [(0,''),(1, 'Enero'), (2, 'Febrero'), (3, 'Marzo'), (4, 'Abril'), (5, 'Mayo'), 
                (6, 'Junio'), (7, 'Julio'),(8, 'Agosto'),(9, 'Septiembre'),(10, 'Octubre'),(11, 'Noviembre'),(12, 'Diciembre')]

class InsurancePremium(models.Model):
    """
    Abstract class of a InsurancePremium
    """
    p_neta = models.DecimalField(max_digits=20, decimal_places=2, default=Decimal("0"), blank=True, null=True)
    derecho = models.DecimalField(max_digits=20, decimal_places=2, default=Decimal("0"), blank=True, null=True)

    rpf = models.DecimalField(max_digits=20, decimal_places=2, default=Decimal("0"), blank=True, null=True) #Exclusivo de polizas
    gastos_investigacion = models.DecimalField(max_digits=20, decimal_places=2, default=Decimal("0"), blank=True, null=True) #Exclusivo de fianzas

    descuento = models.DecimalField(max_digits=20, decimal_places=2, default=Decimal("0"), blank=True, null=True)

    sub_total = models.DecimalField(max_digits=20, decimal_places=2, default=Decimal("0"), blank=True, null=True)
    iva = models.DecimalField(max_digits=20, decimal_places=2, default=Decimal("0"), blank=True, null=True)

    p_total = models.DecimalField(max_digits=20, decimal_places=2, default=Decimal("0"), blank=True, null=True)

    comision = models.DecimalField(max_digits=20, decimal_places=2, default=Decimal("0"), blank=True, null = True)

    # Primas Devengadas Certificados
    p_neta_earned = models.DecimalField(max_digits=20, decimal_places=2, default=Decimal("0"), blank=True, null=True)
    derecho_earned = models.DecimalField(max_digits=20, decimal_places=2, default=Decimal("0"), blank=True, null=True)
    rpf_earned = models.DecimalField(max_digits=20, decimal_places=2, default=Decimal("0"), blank=True, null=True) 
    descuento_earned = models.DecimalField(max_digits=20, decimal_places=2, default=Decimal("0"), blank=True, null=True)
    sub_total_earned = models.DecimalField(max_digits=20, decimal_places=2, default=Decimal("0"), blank=True, null=True)
    iva_earned = models.DecimalField(max_digits=20, decimal_places=2, default=Decimal("0"), blank=True, null=True)
    p_total_earned = models.DecimalField(max_digits=20, decimal_places=2, default=Decimal("0"), blank=True, null=True)
    comision_earned = models.DecimalField(max_digits=20, decimal_places=2, default=Decimal("0"), blank=True, null = True)
    comision_percent_earned = models.DecimalField(max_digits=20, decimal_places=2, default=Decimal("0"), blank=True, null = True)

    #fianza    
    fecha_pago_comision = models.DateTimeField(null=True, blank=True)
    maquila = models.DecimalField(max_digits=20, decimal_places=2, default=Decimal("0"), blank=True, null=True)
    exchange_rate = models.DecimalField(max_digits=20, decimal_places=2, default=Decimal("1"), blank=True, null=True)
    date_maquila = models.DateTimeField(null=True, blank=True)
    year_factura = models.SmallIntegerField(blank=True, null=True)
    date_bono = models.DateTimeField(null=True, blank=True)
    class Meta:
        abstract = True

    """
    Propuesta de recalculo de primas, hay casos que analizar
    def save(self, *args, **kwargs):
        sub_total = self.p_neta + self.derecho + self.rpf + self.gastos_investigacion - self.descuento
        self.sub_total = sub_total
        p_total = float(sub_total) + float(self.iva)
        print('p_total', p_total)
        self.p_total = p_total

        super(InsurancePremium, self).save(*args, **kwargs)
    """

class Validity(models.Model):
    """
    Abstract class of a validity
    """
    start_of_validity = models.DateTimeField(null=True, blank=True)
    end_of_validity = models.DateTimeField(null=True, blank=True)

    class Meta:
        abstract = True

class Polizas(TimeStampedModel, InsurancePremium, Validity):
    contratante_subgroup = models.PositiveIntegerField(blank=True, null=True)
    
    #Numeros de identificación
    folio = models.CharField(max_length=500, blank=True, null=True)
    internal_number = models.CharField(max_length=500, blank=True, null=True)
    poliza_number = models.CharField(max_length=500, blank=True, null=True)
    certificate_number = models.CharField(max_length=500,blank=True, null=True)

    sucursal = models.ForeignKey('core.Sucursales', blank=True, null=True, related_name='policy_sucursal', on_delete=models.CASCADE)

    contractor = models.ForeignKey(Contractor, blank=True, null=True,on_delete=models.CASCADE)#Nueva FK Contractor
    address = models.ForeignKey(Address, blank=True,null=True, on_delete=models.CASCADE)

    aseguradora = models.ForeignKey('aseguradoras.Provider', blank=True,  null=True, related_name="polizas_provider", on_delete=models.CASCADE)
    #Ramos y derivados
    ramo = models.ForeignKey('ramos.Ramos', blank=True, null=True, on_delete=models.CASCADE)
    subramo = models.ForeignKey('ramos.SubRamos', blank=True, null=True, on_delete=models.CASCADE)
    fianza_type = models.ForeignKey('ramos.FianzaType', blank=True, null=True, on_delete=models.CASCADE)

    paquete = models.ForeignKey('paquetes.Package', blank=True,  null=True, on_delete=models.CASCADE)

    clave = models.ForeignKey(Claves, blank=True, null=True, on_delete=models.CASCADE)

    #Status y posibles atributos para fusionar
    status = models.IntegerField(blank=True, null=True, default=1, choices = STATUS)
    #renewed = models.BooleanField(default = False)
    renewed_status = models.IntegerField(blank=True, null=True, default=0, choices = RENEWAL_CHOICES)

    #Exclusivo de certificados, considerar en futuro fusionar con status
    certificado_inciso_activo = models.BooleanField(default = True)

    #Exclusivo de fianzas
    emision_status = models.IntegerField(blank=True, null=True, default=1, choices = STATUS_EMISION)

    forma_de_pago = models.IntegerField(blank=True, null=True, default=12,choices = FORMA_DE_PAGO)   
    owner = models.ForeignKey('auth.User', related_name='poliza', null=True, on_delete=models.CASCADE)
    observations = models.TextField(blank=True, null=True)
    document_type = models.IntegerField(blank=True, null=True, choices = DOCUMENT_TYPE, default=1)

    org_name = models.CharField(max_length=50, null=True, db_index=True)
#    clave = models.ForeignKey(Claves, blank=True, null=True, on_delete=models.CASCADE)
    f_currency = models.IntegerField(null = True, blank=True, default = 1,choices = F_CURRENCY_CHOICES)
    identifier = models.CharField(max_length=500, blank=True, null=True)
    administration_type = models.IntegerField(null = True, default = 1, choices = ADMINISTRATION_TYPES)
    name = models.CharField(max_length=500, blank=True, null=True)
    parent = models.ForeignKey('self', blank=True, null=True, on_delete=models.CASCADE)
    hospital_level = models.CharField(max_length=500, blank=True, null=True)

    comision_percent = models.DecimalField(max_digits=20, decimal_places=2, default=Decimal("0"), null = True)

    #Atributo exclusivo de poliza de grupo y colectividad (confirmar este ultimo )es un atributo que se puede quitar pero implica muchos cambios en las vistas
    caratula = models.CharField(max_length=500, blank=True, null=True)

    total_receipts = models.IntegerField(default=0, blank=True, null=True)
    give_comision = models.DecimalField(max_digits=20, decimal_places=2, default=Decimal("0"), null = True)
    migrated = models.BooleanField(default = False)

    responsable = models.ForeignKey('auth.User', blank=True, null=True, related_name='policy_responsable', on_delete=models.CASCADE)
    collection_executive = models.ForeignKey('auth.User', blank=True, null=True, related_name='policy_collection_executive', on_delete=models.CASCADE)

    fecha_baja_inciso = models.DateTimeField(null=True, blank=True)
    rec_antiguedad = models.DateTimeField(null=True, blank=True)
    contacto = models.CharField(max_length=500, blank=True, null=True)
    
    is_renewable = models.IntegerField(blank = True, null = True, choices = RENEWED_CHOICES, default = 1)
    receipts_by = models.IntegerField(blank = True, null = True, choices = RECEIPTS_CHOICES, default = 1)
    reason_ren = models.TextField(blank=True, null=True)
    reason_cancel = models.TextField(blank=True, null=True)
    
    tabulator = models.CharField(max_length=500, blank=True, null=True)
    emision_date = models.DateTimeField(null=True, blank=True)

    reason_rehabilitate = models.TextField(blank=True, null=True)
    concept_annulment = models.IntegerField(blank=True, null=True, default=0, choices = CONCEPT_CHOICES)
    fecha_anuencia =  models.DateTimeField(null=True, blank=True)
    deductible = models.TextField(blank=True, null=True)

    track_bitacora = models.BooleanField(default = False)
    scheme = models.IntegerField(blank=True, null=True, default=1, choices = PAYSCHEME_CHOICES)
    accident_rate = models.DecimalField(max_digits=20, decimal_places=2, default=Decimal("0"), null = True, blank = True)
    steps = JSONField(null = True, blank =True, default = "", max_length=6000)
    
    business_line = models.IntegerField(blank=True, null=True, default=0, choices = BUSINESSLINE_CHOICES)

    type_policy = models.IntegerField(blank=True, null=True, default=1, choices = TIPOPOLICY_CHOICES)
    celula = models.ForeignKey(CelulaContractor, blank=True, null=True, related_name='poliza_celula', on_delete=models.CASCADE)
    groupinglevel = models.ForeignKey(GroupingLevel, blank=True, null=True, related_name='poliza_groupinglevel', on_delete=models.CASCADE)

    conducto_de_pago = models.IntegerField(choices = DOMICILIADA_CHOICES, default = 1)
    bono_variable = models.DecimalField(max_digits=20, decimal_places=2, default=Decimal("0"), null = True, blank = True)

    has_programa_de_proveedores = models.BooleanField(default=False)
    # Alta Fecha    
    charge_date = models.DateTimeField(null=True, blank=True)
    date_cancel = models.DateTimeField(null=True, blank=True)
    owner_cancel = models.ForeignKey('auth.User', related_name='owner_cancel_poliza', null=True, on_delete=models.CASCADE)
    programa_de_proveedores_contractor = models.ForeignKey(Contractor, blank=True, null=True, related_name = 'contractor_programa_de_proveedores', on_delete=models.CASCADE)

    state_circulation = models.CharField(max_length=500, blank=True, null=True)

    monto_cancelacion = models.DecimalField(max_digits=20, decimal_places=2, null = True, blank = True )
    fecha_cancelacion = models.DateTimeField(null=True, blank=True)
    date_emision_factura = models.DateTimeField(null=True, blank=True)
    month_factura = models.IntegerField(blank=True, null=True, default=0, choices = MONTH_CHOICES)
    folio_factura = models.CharField(max_length=500, blank=True, null=True)

    fecha_entrega = models.DateTimeField(null=True, blank=True)
    cancelnotas = models.BooleanField(default = False)
    #polizas contributorias
    contributory = models.BooleanField(default = False)        
    rfc_cve = models.CharField(max_length=500, blank=True, null=True)
    rfc_homocve = models.CharField(max_length=500, blank=True, null=True)
    dom_callenum = models.CharField(max_length=500, blank=True, null=True)
    dom_colonia = models.CharField(max_length=500, blank=True, null=True)
    dom_cp = models.CharField(max_length=500, blank=True, null=True)
    dom_poblacion = models.CharField(max_length=500, blank=True, null=True)
    dom_estado = models.CharField(max_length=500, blank=True, null=True)
    scraper =  models.BooleanField(default = False)  
    from_pdf =  models.BooleanField(default = False, blank=True)  
    from_task =  models.BooleanField(default = False, blank=True)  
    task_associated = models.IntegerField(default=0, blank=True, null=True)
    comision_derecho_percent = models.DecimalField(max_digits=20, decimal_places=2, default=Decimal("0"), null = True)
    comision_rpf_percent = models.DecimalField(max_digits=20, decimal_places=2, default=Decimal("0"), null = True)
    # def __str__(self):
    #     return self

    def get_certificates(self):
        """Propuesta de metodo para obtener los certificados mediante al atributo caratula"""
        certificados = Polizas.objects.filter(caratula=self.id, document_type=6)        
        return certificados

    def get_certificates_v2(self):
        """Propuesta de metodo para obtener los certificados mediante el FK parent"""
        subgrupos = Polizas.objects.filter(parent=self)
        categorias = Polizas.objects.filter(parent=subgrupos)
        certificados = Polizas.objects.filter(parent=categorias)
        return certificados

    def update_renewed_status(self):
        """
        Actualiza el atributo renewed_status
        y status en caso de que renewed_status sea igual 1 (renovación completada)
        """
        try:
            renovation = OldPolicies.objects.get(base_policy_id=self.id)
            process_renovation = renovation.is_in_process_renovation()

            if process_renovation == True:
                renewed_status = 2
            else:
                renewed_status = 1

        except OldPolicies.DoesNotExist:
            renewed_status = 0
        except OldPolicies.MultipleObjectsReturned:
            #Existen varias renovaciones
            renewed_status = 3

        self.renewed_status = renewed_status

        if renewed_status == 1 and self.status != 14:
            self.status = 12

    def convert_status_string_status_int(self, status_string):

        if status_string == 'No renovada':
            status_string = 'No Renovada'
            
        for status in STATUS:
            if status[1] == status_string:
                return status[0]
        return 'Status no encontrado'

    #Descomentar una vez corrido el script 2686            
#    def save(self, *args, **kwargs):

#        self.update_renewed_status()
#        super(Polizas, self).save(*args, **kwargs)


@receiver(post_save, sender=Polizas)
def update_renewed_status_policy_previous(sender, instance, created, **kwargs):
    try:
        renovation = OldPolicies.objects.get(new_policy_id=instance.pk)

        poliza_anterior = renovation.base_policy
        return None

    except OldPolicies.DoesNotExist:
#        print('No hay polizas anteriores')
        return None

    except OldPolicies.MultipleObjectsReturned:
        return None


class OldPolicies(TimeStampedModel):
    base_policy = models.ForeignKey(Polizas, related_name="old_policies", blank=True, null=True, on_delete=models.CASCADE)
    new_policy = models.ForeignKey(Polizas, related_name="poliza_prev", blank=True, null=True, on_delete=models.CASCADE)
    org_name = models.CharField(max_length=50, null=True, db_index=True)
    owner = models.ForeignKey('auth.User', related_name='old_policies', null=True, on_delete=models.CASCADE)

    def __str__(self):
        try:
            return self.base_policy.poliza_number
        except:
            return 'Sin poliza base'

    def is_in_process_renovation(self):
        """
        Mientras la poliza nueva sea una OT (document_type = 1),.
        esta en proceso de renovación
        """

        if self.new_policy.status == 1:
            return True
        else:
            return False

class Assign(TimeStampedModel):
    user = models.ForeignKey('auth.User',blank=True,null=True, related_name='assign', on_delete=models.CASCADE)
    poliza = models.ForeignKey(Polizas, null=True,blank=True, related_name='policy_assign', on_delete=models.CASCADE)
    is_owner = models.BooleanField(default = False)
    active = models.BooleanField(default=True)

    def __str__(self):
        return self.poliza


class Pendients(TimeStampedModel):
    email = models.EmailField(max_length=50,blank=True,null=True)
    poliza = models.ForeignKey(Polizas, null=True,blank=True, related_name='pendient_assign', on_delete=models.CASCADE)
    is_owner = models.BooleanField(default = False)
    active = models.BooleanField(default=True)





class Cotizacion(TimeStampedModel):
    contractor = models.ForeignKey(Contractor, blank=True, null=True,on_delete=models.CASCADE)#Nueva FK Contracto, on_delete=models.CASCADE
    org_name = models.CharField(max_length=50, null=True, db_index=True)
    owner = models.ForeignKey('auth.User', related_name='cotizacion', null=True, on_delete=models.CASCADE)
    status = models.IntegerField(blank=True, null=True, default=1, choices = STATUS_COT)
    observations = models.TextField(blank=True, null=True)
    main_parent = models.IntegerField(default = 0)

    ramo_code = models.IntegerField(blank=True, null=True)
    subramo_code = models.IntegerField(blank=True, null=True)

    prospecto = models.IntegerField(default=1)
    life = JSONField(null = True, blank =True, default = "", max_length=6000)
    accidents = JSONField(null = True, blank =True, default = "", max_length=6000)
    danios = JSONField(null = True, blank =True, default = "", max_length=6000)
    auto = JSONField(null = True, blank =True, default = "", max_length=6000)
    first_name = models.CharField(max_length=500, null=True, blank=True)
    last_name = models.CharField(max_length=500, null=True, blank=True)
    second_last_name = models.CharField(max_length=500, null=True, blank=True)
    email = models.CharField(max_length=500, null=True, blank=True)
    phone = models.CharField(max_length=500, null=True, blank=True)
    ramo = JSONField(null = True, blank =True, default = "", max_length=6000)
    subramo = JSONField(null = True, blank =True, default = "", max_length=6000)
    tipo = JSONField(null = True, blank =True, default = "", max_length=6000)
    aseguradora = ArrayField(JSONField(max_length=6000), default = [], null = True, blank = True)
    aseguradora_seleccionada = models.CharField(max_length=500, null=True)
    is_complete = models.BooleanField(default = False)



class AseguradorasCotizacionPrimas(TimeStampedModel):
    cotizacion = models.ForeignKey(Cotizacion, blank=True, null=True, related_name="cotizacion_primas", on_delete=models.CASCADE)
    org_name = models.CharField(max_length=50, null=True, db_index=True)
    aseguradora = models.CharField(max_length=50, null=True, db_index=True)
    prima = models.CharField(max_length=50, null=True, db_index=True)
    checked = models.BooleanField(default=False, null=False)

class AseguradorasInvolved(TimeStampedModel):
    aseguradora = models.ForeignKey('aseguradoras.Provider', blank=True,  null=True, on_delete=models.CASCADE)
    cotizacion = models.ForeignKey(Cotizacion, blank=True, null=True, related_name="cotizacion_providers", on_delete=models.CASCADE)
    org_name = models.CharField(max_length=50, null=True, db_index=True)
    owner = models.ForeignKey('auth.User', related_name='aseg_invol', null=True, on_delete=models.CASCADE)
    cot_id = models.IntegerField(default = 0)
    car_descr = models.TextField(blank=True, null=True)
