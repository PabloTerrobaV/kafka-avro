#!/usr/bin/env python3
import json
import sys
from avro.schema import parse

def cargar_esquema(archivo):
    with open(archivo, 'r') as f:
        return parse(f.read())

def comparar_esquemas(esquema_anterior, esquema_nuevo):
    campos_anteriores = {campo.name: campo for campo in esquema_anterior.fields}
    campos_nuevos = {campo.name: campo for campo in esquema_nuevo.fields}

    campos_añadidos = []
    campos_eliminados = []
    campos_modificados = []

    # Detectar campos añadidos
    for nombre in campos_nuevos:
        if nombre not in campos_anteriores:
            campo = campos_nuevos[nombre]
            tiene_default = 'default' in campo.props
            campos_añadidos.append({
                'nombre': nombre,
                'tipo': str(campo.type),
                'tiene_default': tiene_default,
                'default': campo.props.get('default', None) if tiene_default else None
            })

    # Detectar campos eliminados
    for nombre in campos_anteriores:
        if nombre not in campos_nuevos:
            campo = campos_anteriores[nombre]
            tiene_default = 'default' in campo.props
            campos_eliminados.append({
                'nombre': nombre,
                'tipo': str(campo.type),
                'tiene_default': tiene_default,
                'default': campo.props.get('default', None) if tiene_default else None
            })

    # Detectar campos modificados
    for nombre in campos_anteriores:
        if nombre in campos_nuevos and str(campos_anteriores[nombre].type) != str(campos_nuevos[nombre].type):
            campos_modificados.append({
                'nombre': nombre,
                'tipo_anterior': str(campos_anteriores[nombre].type),
                'tipo_nuevo': str(campos_nuevos[nombre].type),
                'tiene_default_anterior': 'default' in campos_anteriores[nombre].props,
                'tiene_default_nuevo': 'default' in campos_nuevos[nombre].props
            })

    # Imprimir resultados
    print("=== CAMBIOS DETECTADOS ===")

    if campos_añadidos:
        print("\n🟢 CAMPOS AÑADIDOS:")
        for campo in campos_añadidos:
            info_default = f" (con valor por defecto: {campo['default']})" if campo['tiene_default'] else " (sin valor por defecto)"
            print(f"  + {campo['nombre']} ({campo['tipo']}){info_default}")

    if campos_eliminados:
        print("\n🔴 CAMPOS ELIMINADOS:")
        for campo in campos_eliminados:
            info_default = f" (con valor por defecto: {campo['default']})" if campo['tiene_default'] else " (sin valor por defecto)"
            print(f"  - {campo['nombre']} ({campo['tipo']}){info_default}")

    if campos_modificados:
        print("\n🟠 CAMPOS MODIFICADOS:")
        for campo in campos_modificados:
            print(f"  ~ {campo['nombre']}: {campo['tipo_anterior']} → {campo['tipo_nuevo']}")

    # Resumen
    print("\n=== RESUMEN ===")
    print(f"Campos añadidos: {len(campos_añadidos)}")
    print(f"Campos eliminados: {len(campos_eliminados)}")
    print(f"Campos modificados: {len(campos_modificados)}")
    print(f"Total de cambios: {len(campos_añadidos) + len(campos_eliminados) + len(campos_modificados)}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python compare_schemas.py <esquema_anterior.avsc> <esquema_nuevo.avsc>")
        sys.exit(1)

    esquema_anterior_file = sys.argv[1]
    esquema_nuevo_file = sys.argv[2]

    try:
        esquema_anterior = cargar_esquema(esquema_anterior_file)
        esquema_nuevo = cargar_esquema(esquema_nuevo_file)

        comparar_esquemas(esquema_anterior, esquema_nuevo)
        sys.exit(0)  # Añade este código de salida explícito
    except Exception as e:
        # print(f"Error: {e}")
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)