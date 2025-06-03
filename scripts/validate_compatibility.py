#!/usr/bin/env python3
import sys
import requests
from avro.schema import parse

def cargar_esquema(archivo):
    with open(archivo, 'r') as f:
        return parse(f.read())

def obtener_compatibilidad(schema_registry_url, subject_name):
    try:
        response = requests.get(f"{schema_registry_url}/config/{subject_name}", timeout=10)
        if response.status_code == 200:
            return response.json()['compatibilityLevel']
        response = requests.get(f"{schema_registry_url}/config", timeout=10)
        if response.status_code == 200:
            return response.json().get('compatibilityLevel', 'BACKWARD')
        return 'BACKWARD'
    except Exception as e:
        print(f"❌ Error al obtener compatibilidad: {e}")
        sys.exit(1)

def validar_metadatos(esquema_ant, esquema_nuevo):
    errores = []
    advertencias = []

    # Validar 'type'
    if getattr(esquema_ant, 'type', None) != getattr(esquema_nuevo, 'type', None):
        errores.append(f"Cambio de 'type' de '{getattr(esquema_ant, 'type', None)}' a '{getattr(esquema_nuevo, 'type', None)}' no es compatible.")

    # Validar 'name'
    if getattr(esquema_ant, 'name', None) != getattr(esquema_nuevo, 'name', None):
        errores.append(f"Cambio de 'name' de '{getattr(esquema_ant, 'name', None)}' a '{getattr(esquema_nuevo, 'name', None)}' no es compatible. Usa aliases si es necesario.")

    # Validar 'namespace'
    if getattr(esquema_ant, 'namespace', None) != getattr(esquema_nuevo, 'namespace', None):
        advertencias.append(f"Cambio de 'namespace' de '{getattr(esquema_ant, 'namespace', None)}' a '{getattr(esquema_nuevo, 'namespace', None)}'. Puede causar problemas de deserialización en clientes Java. Considera el uso de aliases.")

    return errores, advertencias

def analizar_cambios(esquema_ant, esquema_nuevo):
    campos_anteriores = {campo.name: campo for campo in esquema_ant.fields}
    campos_nuevos = {campo.name: campo for campo in esquema_nuevo.fields}

    cambios = {
        'añadidos_sin_default': [],
        'eliminados_sin_default': [],
        'añadidos_con_default': [],
        'eliminados_con_default': [],
        'modificados': []
    }

    for nombre in campos_nuevos:
        if nombre not in campos_anteriores:
            campo = campos_nuevos[nombre]
            if campo.has_default:
                cambios['añadidos_con_default'].append(nombre)
            else:
                cambios['añadidos_sin_default'].append(nombre)

    for nombre in campos_anteriores:
        if nombre not in campos_nuevos:
            campo = campos_anteriores[nombre]
            if campo.has_default:
                cambios['eliminados_con_default'].append(nombre)
            else:
                cambios['eliminados_sin_default'].append(nombre)

    for nombre in campos_anteriores:
        if nombre in campos_nuevos:
            ant = campos_anteriores[nombre]
            nue = campos_nuevos[nombre]
            if ant != nue:
                cambios['modificados'].append(nombre)

    return cambios

def validar_compatibilidad(cambios, compatibilidad):
    errores = []
    advertencias = []

    if cambios['modificados']:
        errores.append(f"Modificación de campos existentes no permitida: {cambios['modificados']}")

    if compatibilidad.startswith('BACKWARD'):
        if cambios['añadidos_sin_default']:
            errores.append(
                f"BACKWARD: Campos añadidos sin valor por defecto: {cambios['añadidos_sin_default']}. "
                "Deben tener 'default' o usar FORWARD compatibility."
            )
    if compatibilidad.startswith('FORWARD'):
        if cambios['eliminados_sin_default']:
            errores.append(
                f"FORWARD: Campos eliminados sin valor por defecto: {cambios['eliminados_sin_default']}. "
                "Deben mantenerse o usar BACKWARD compatibility."
            )
    if compatibilidad.startswith('FULL'):
        if cambios['añadidos_sin_default'] or cambios['eliminados_sin_default']:
            errores.append(
                f"FULL: No se permiten cambios incompatibles. "
                f"Añadidos sin default: {cambios['añadidos_sin_default']}. "
                f"Eliminados sin default: {cambios['eliminados_sin_default']}."
            )

    if cambios['añadidos_con_default']:
        advertencias.append(f"Campos añadidos con valor por defecto: {cambios['añadidos_con_default']}")
    if cambios['eliminados_con_default']:
        advertencias.append(f"Campos eliminados con valor por defecto: {cambios['eliminados_con_default']}")

    return errores, advertencias

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python validate_compatibility.py <esquema_anterior.avsc> <esquema_nuevo.avsc>")
        sys.exit(1)

    try:
        esquema_ant = cargar_esquema(sys.argv[1])
        esquema_nuevo = cargar_esquema(sys.argv[2])

        schema_registry_url = "http://schema-registry:8081"
        subject_name = "orders-value"
        compatibilidad = obtener_compatibilidad(schema_registry_url, subject_name)
        print(f"🔍 Modo de compatibilidad: {compatibilidad}")

        # Validación de metadatos iniciales
        errores_meta, advertencias_meta = validar_metadatos(esquema_ant, esquema_nuevo)
        if errores_meta:
            print("\n❌ Errores de metadatos:")
            for e in errores_meta:
                print(f" - {e}")
            sys.exit(1)
        if advertencias_meta:
            print("\n⚠️ Advertencias de metadatos:")
            for a in advertencias_meta:
                print(f" - {a}")

        # Análisis y validación de campos
        cambios = analizar_cambios(esquema_ant, esquema_nuevo)
        print("📊 Cambios detectados:")
        print(f" - Añadidos sin default: {cambios['añadidos_sin_default']}")
        print(f" - Eliminados sin default: {cambios['eliminados_sin_default']}")
        print(f" - Añadidos con default: {cambios['añadidos_con_default']}")
        print(f" - Eliminados con default: {cambios['eliminados_con_default']}")
        print(f" - Modificados: {cambios['modificados']}")

        errores, advertencias = validar_compatibilidad(cambios, compatibilidad)

        if advertencias:
            print("\n⚠️ Advertencias:")
            for a in advertencias:
                print(f" - {a}")

        if errores:
            print("\n❌ Errores de compatibilidad:")
            for e in errores:
                print(f" - {e}")
            sys.exit(1)

        print("\n✅ El esquema es compatible según las reglas configuradas")
        sys.exit(0)

    except Exception as e:
        print(f"\n❌ Error crítico: {str(e)}")
        sys.exit(1)



"""
#!/usr/bin/env python3
import sys
import requests
from avro.schema import parse

def validar_metadatos(cambios_metadatos, compatibilidad):
    errores = []
    advertencias = []

    # Reglas para 'name'
    if 'name' in cambios_metadatos:
        if compatibilidad != 'NONE':
            errores.append("Cambio de 'name' requiere compatibilidad=NONE")
        else:
            advertencias.append("Cambio de 'name' detectado (usar aliases para compatibilidad)")

    # Reglas para 'type'
    if 'type' in cambios_metadatos:
        errores.append("Cambio de 'type' es incompatible con cualquier modo de compatibilidad")

    # Reglas para 'namespace'
    if 'namespace' in cambios_metadatos:
        advertencias.append("Cambio de 'namespace' puede afectar serialización (usar aliases)")

    return errores, advertencias

def obtener_compatibilidad(url_registry, subject):
    try:
        response = requests.get(f"{url_registry}/config/{subject}")
        if response.status_code == 200:
            return response.json()['compatibilityLevel']

        response_global = requests.get(f"{url_registry}/config")
        return response_global.json().get('compatibilityLevel', 'BACKWARD')

    except Exception as e:
        print(f"⚠️ Error obteniendo compatibilidad: {e}")
        return 'BACKWARD'

def analizar_campos(esquema_ant, esquema_nuevo):
    campos_ant = {c.name: c for c in esquema_ant.fields}
    campos_nue = {c.name: c for c in esquema_nuevo.fields}

    return {
        'añadidos_sin_default': [n for n in campos_nue if n not in campos_ant and not campos_nue[n].has_default],
        'eliminados_sin_default': [n for n in campos_ant if n not in campos_nue and not campos_ant[n].has_default]
    }

def validar_reglas_campos(cambios_campos, compatibilidad):
    requerida = None

    if cambios_campos['eliminados_sin_default']:
        requerida = 'BACKWARD' if 'BACKWARD' in compatibilidad else None
    elif cambios_campos['añadidos_sin_default']:
        requerida = 'FORWARD' if 'FORWARD' in compatibilidad else None

    return requerida

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python validate_compatibility.py <esquema_ant> <esquema_nuevo>")
        sys.exit(1)

    try:
        # Cargar esquemas
        esquema_ant = parse(open(sys.argv[1]).read())
        esquema_nuevo = parse(open(sys.argv[2]).read())

        # Configuración
        registry_url = "http://schema-registry:8081"
        subject = "orders-value"

        # Obtener compatibilidad
        compatibilidad = obtener_compatibilidad(registry_url, subject)
        print(f"🔍 Modo de compatibilidad: {compatibilidad}")

        # Validar metadatos
        cambios_metadatos = {
            'type': (esquema_ant.type, esquema_nuevo.type),
            'name': (esquema_ant.name, esquema_nuevo.name),
            'namespace': (esquema_ant.namespace, esquema_nuevo.namespace),
            'doc': (getattr(esquema_ant, 'doc', None), getattr(esquema_nuevo, 'doc', None))
        }
        cambios_metadatos = {k: v for k, v in cambios_metadatos.items() if v[0] != v[1]}

        errores, advertencias = validar_metadatos(cambios_metadatos, compatibilidad)

        # Validar campos
        cambios_campos = analizar_campos(esquema_ant, esquema_nuevo)
        compatibilidad_requerida = validar_reglas_campos(cambios_campos, compatibilidad)

        # Resultados
        if errores:
            print("❌ Errores de compatibilidad:")
            for e in errores: print(f"  - {e}")

        if advertencias:
            print("⚠️ Advertencias:")
            for a in advertencias: print(f"  - {a}")

        if compatibilidad_requerida and compatibilidad_requerida not in compatibilidad:
            print(f"❌ Compatibilidad requerida: {compatibilidad_requerida}")
            sys.exit(1)

        if errores:
            sys.exit(1)

        print("✅ Validación completada exitosamente")
        sys.exit(0)

    except Exception as e:
        print(f"❌ Error crítico: {e}")
        sys.exit(1)
"""


"""
#!/usr/bin/env python3
import json
import sys
import requests
from avro.schema import parse

def cargar_esquema(archivo):
    with open(archivo, 'r') as f:
        return parse(f.read())

def obtener_compatibilidad(schema_registry_url, subject_name):
    url = f"{schema_registry_url}/config/{subject_name}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()['compatibilityLevel']
    else:
        # Si no hay configuración específica, obtener la global
        url = f"{schema_registry_url}/config"
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()['compatibilityLevel']
        else:
            # Si no hay configuración global, usar BACKWARD como predeterminado
            return "BACKWARD"

def analizar_esquemas(esquema_anterior, esquema_nuevo):
    campos_anteriores = {campo.name: campo for campo in esquema_anterior.fields}
    campos_nuevos = {campo.name: campo for campo in esquema_nuevo.fields}

    # Detectar campos añadidos sin valor por defecto
    campos_añadidos_sin_default = [
        nombre for nombre in campos_nuevos
        if nombre not in campos_anteriores and 'default' not in campos_nuevos[nombre].props
    ]

    # Detectar campos eliminados sin valor por defecto
    campos_eliminados_sin_default = [
        nombre for nombre in campos_anteriores
        if nombre not in campos_nuevos and 'default' not in campos_anteriores[nombre].props
    ]

    # Detectar campos añadidos con valor por defecto
    campos_añadidos_con_default = [
        nombre for nombre in campos_nuevos
        if nombre not in campos_anteriores and 'default' in campos_nuevos[nombre].props
    ]

    # Detectar campos eliminados con valor por defecto
    campos_eliminados_con_default = [
        nombre for nombre in campos_anteriores
        if nombre not in campos_nuevos and 'default' in campos_anteriores[nombre].props
    ]

    return {
        'campos_añadidos_sin_default': campos_añadidos_sin_default,
        'campos_eliminados_sin_default': campos_eliminados_sin_default,
        'campos_añadidos_con_default': campos_añadidos_con_default,
        'campos_eliminados_con_default': campos_eliminados_con_default
    }

def validar_compatibilidad(cambios, compatibilidad):
    print(f"Compatibilidad configurada: {compatibilidad}")

    hay_campos_añadidos_sin_default = len(cambios['campos_añadidos_sin_default']) > 0
    hay_campos_eliminados_sin_default = len(cambios['campos_eliminados_sin_default']) > 0
    hay_campos_opcionales_cambiados = (len(cambios['campos_añadidos_con_default']) > 0 or
                                       len(cambios['campos_eliminados_con_default']) > 0)

    # Caso 1: Si se eliminan campos SIN valor por defecto
    if hay_campos_eliminados_sin_default and not hay_campos_añadidos_sin_default:
        compatibilidad_requerida = "BACKWARD"
        if compatibilidad in ['BACKWARD', 'BACKWARD_TRANSITIVE', 'FULL', 'FULL_TRANSITIVE']:
            print(f"✅ Compatible: Se eliminaron campos sin valor por defecto y la compatibilidad es {compatibilidad}")
        else:
            print(f"❌ Incompatible: Se eliminaron campos sin valor por defecto pero la compatibilidad es {compatibilidad}")
            sys.exit(1)

    # Caso 2: Si se añaden campos SIN valor por defecto
    elif hay_campos_añadidos_sin_default and not hay_campos_eliminados_sin_default:
        compatibilidad_requerida = "FORWARD"
        if compatibilidad in ['FORWARD', 'FORWARD_TRANSITIVE', 'FULL', 'FULL_TRANSITIVE']:
            print(f"✅ Compatible: Se añadieron campos sin valor por defecto y la compatibilidad es {compatibilidad}")
        else:
            print(f"❌ Incompatible: Se añadieron campos sin valor por defecto pero la compatibilidad es {compatibilidad}")
            sys.exit(1)

    # Caso 3: Si se añaden/eliminan campos CON valor por defecto (opcionales)
    elif hay_campos_opcionales_cambiados and not hay_campos_añadidos_sin_default and not hay_campos_eliminados_sin_default:
        compatibilidad_requerida = "Cualquiera"
        print("✅ Compatible: Solo se modificaron campos opcionales (con valor por defecto)")

    # Caso 4: Si se eliminan campos SIN valor por defecto + se añaden/eliminan campos CON valor por defecto
    elif hay_campos_eliminados_sin_default and hay_campos_opcionales_cambiados:
        compatibilidad_requerida = "BACKWARD"
        if compatibilidad in ['BACKWARD', 'BACKWARD_TRANSITIVE', 'FULL', 'FULL_TRANSITIVE']:
            print(f"✅ Compatible: Combinación de cambios requiere BACKWARD y la compatibilidad es {compatibilidad}")
        else:
            print(f"❌ Incompatible: Combinación de cambios requiere BACKWARD pero la compatibilidad es {compatibilidad}")
            sys.exit(1)

    # Caso 5: Si se añaden campos SIN valor por defecto + se añaden/eliminan campos CON valor por defecto
    elif hay_campos_añadidos_sin_default and hay_campos_opcionales_cambiados:
        compatibilidad_requerida = "FORWARD"
        if compatibilidad in ['FORWARD', 'FORWARD_TRANSITIVE', 'FULL', 'FULL_TRANSITIVE']:
            print(f"✅ Compatible: Combinación de cambios requiere FORWARD y la compatibilidad es {compatibilidad}")
        else:
            print(f"❌ Incompatible: Combinación de cambios requiere FORWARD pero la compatibilidad es {compatibilidad}")
            sys.exit(1)

    # Sin cambios o caso no contemplado
    else:
        compatibilidad_requerida = "No determinada"
        print("✓ No se detectaron cambios significativos o el caso no está contemplado")

    print(f"Compatibilidad requerida por los cambios: {compatibilidad_requerida}")
    print("✅ Validación completada.")

    # Devolvemos la compatibilidad requerida para que pueda ser usada por el pipeline
    print(compatibilidad_requerida)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python validate_compatibility.py <esquema_anterior.avsc> <esquema_nuevo.avsc>")
        sys.exit(1)

    esquema_anterior_file = sys.argv[1]
    esquema_nuevo_file = sys.argv[2]

    try:
        esquema_anterior = cargar_esquema(esquema_anterior_file)
        esquema_nuevo = cargar_esquema(esquema_nuevo_file)

        # Obtenemos estos valores del entorno (configurados en el Jenkins)
        schema_registry_url = "http://schema-registry:8081"
        subject_name = "orders-value"

        compatibilidad = obtener_compatibilidad(schema_registry_url, subject_name)
        cambios = analizar_esquemas(esquema_anterior, esquema_nuevo)

        validar_compatibilidad(cambios, compatibilidad)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
"""