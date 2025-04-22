pipeline {
    agent any

    // Definición de variables de entorno, que se usarán en los comandos
    environment {
        // URL del Schema Registry (accesible desde Jenkins en la red de Docker)
        SCHEMA_REGISTRY_URL = 'http://schema-registry:8081'
        // El subject que se usará para consultar la configuración de compatibilidad en Schema Registry
        SUBJECT_NAME = 'orders-value'
        // URL del repositorio Git donde se encuentra el esquema actualizado
        GITHUB_REPO_URL = 'https://github.com/PabloTerrobaV/kafka-avro.git'
        // Rama del repositorio
        GITHUB_BRANCH = 'main'
        // Ruta relativa del esquema dentro del proyecto (nuevo esquema que se obtendrá del repo)
        SCHEMA_PATH = 'common/src/main/avro/Order.avsc'
    }

    stages {
        stage('Descargar versión antigua del esquema') {
            steps {
                echo 'Descargando versión antigua del esquema desde Schema Registry...'
                // Se descarga el esquema antiguo utilizando el endpoint /versions/latest (sin /schema) y se extrae el campo "schema" con jq
                sh """
                curl -v ${SCHEMA_REGISTRY_URL}/subjects/${SUBJECT_NAME}/versions/latest | jq -r .schema > old_schema.avsc || {
                    echo "Error al descargar el esquema antiguo"
                    exit 1
                }
                """
            }
        }

        stage('Obtener nueva versión del esquema') {
            steps {
                echo 'Descargando nueva versión del esquema desde GitHub...'
                // Realiza el checkout del repositorio para obtener el archivo actualizado
                checkout([
                    $class: 'GitSCM',
                    branches: [[name: "${GITHUB_BRANCH}"]],
                    userRemoteConfigs: [[url: "${GITHUB_REPO_URL}"]]
                ])
                // Copia el archivo del esquema actualizado a new_schema.avsc
                sh "cp ${SCHEMA_PATH} new_schema.avsc || { echo 'Error al copiar el nuevo esquema'; exit 1; }"
            }
        }

        stage('Inspeccionar esquemas') {
            steps {
                // Verifica que ambos archivos existan y se muestran sus contenidos
                sh '''
                echo "🔍 Verificando existencia y contenido de los archivos AVSC..."

                if [ -f old_schema.avsc ]; then
                  echo "✅ old_schema.avsc existe:"
                  cat old_schema.avsc
                else
                  echo "❌ old_schema.avsc no se encuentra"
                fi

                if [ -f new_schema.avsc ]; then
                  echo "✅ new_schema.avsc existe:"
                  cat new_schema.avsc
                else
                  echo "❌ new_schema.avsc no se encuentra"
                fi
                '''
            }
        }

        stage('Comparar esquemas y detectar cambios') {
            steps {
                echo 'Comparando esquemas y detectando cambios...'
                // Se muestra información de depuración: se listan los archivos en "scripts" y se muestra el contenido de ambos esquemas
                sh '''
                echo "[DEBUG] Archivos disponibles en scripts/"
                ls -l scripts

                echo "[DEBUG] Contenido de old_schema.avsc:"
                cat old_schema.avsc || echo "No se encontró old_schema.avsc"

                echo "[DEBUG] Contenido de new_schema.avsc:"
                cat new_schema.avsc || echo "No se encontró new_schema.avsc"

                echo "[DEBUG] Ejecutando script de comparación..."
                python3 scripts/compare_schemas.py old_schema.avsc new_schema.avsc > schema_diff.txt || {
                    echo "[ERROR] Error durante la comparación"
                    cat schema_diff.txt || echo "schema_diff.txt no se creó"
                    exit 1
                }

                echo "[DEBUG] Contenido de schema_diff.txt:"
                cat schema_diff.txt
                '''
                script {
                    def changes = readFile('schema_diff.txt')
                    echo "Cambios detectados:\n${changes}"
                }
            }
        }

        // ******* Stage de validación de compatibilidad de los esquemas, a través de script de Python *******
        stage('Validar compatibilidad del esquema') {
            steps {
                echo 'Validando compatibilidad del esquema...'
                // Se ejecuta el script Python de validación que:
                // 1. Consulta la configuración de compatibilidad (usando el Schema Registry).
                // 2. Analiza los cambios entre el esquema anterior y el nuevo.
                // 3. Valida la compatibilidad y, en caso de incompatibilidad, fuerza la salida con exit code 1.
                sh '''
                python3 scripts/validate_compatibility.py old_schema.avsc new_schema.avsc || {
                    echo "[ERROR] La validación de compatibilidad ha fallado"
                    exit 1
                }
                '''
            }
        }

        // ******* Nuevo stage: Añadir fecha y hora en el nuevo esquema para solucionar el problema de las duplicidades *******
        stage('Forzar modificación del esquema (opcional)') {
            steps {
                echo 'Ajustando el nuevo esquema para forzar el registro de una nueva versión...'
                sh '''
                    TIMESTAMP=$(date "+%Y-%m-%d %H:%M:%S")
                    sed -i "s/\\"namespace\\" *: *\\"com.example.kafka\\"/\\"namespace\\": \\"com.example.kafka\\", \\"doc\\": \\"Actualizado el $TIMESTAMP\\"/" new_schema.avsc
                    echo "Contenido de new_schema.avsc modificado:"
                    cat new_schema.avsc
                '''
            }
        }

        stage('Registrar esquema en Schema Registry') {
            steps {
                echo 'Registrando nuevo esquema en Schema Registry...'
                script {
                    def response = sh(
                        script: '''
                        echo "[DEBUG] Generando payload JSON con jq..."
                        jq -Rs '{schema: .}' new_schema.avsc > payload.json

                        echo "[DEBUG] Payload de registro:"
                        cat payload.json

                        echo "[DEBUG] Ejecutando registro con curl..."
                        curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \\
                             --data @payload.json \\
                             ${SCHEMA_REGISTRY_URL}/subjects/${SUBJECT_NAME}/versions
                        ''',
                        returnStdout: true
                    ).trim()

                    echo "Respuesta del registro: ${response}"

                    def newVersion = readJSON text: response
                    echo "Esquema registrado (o ya existente) en la versión: ${newVersion.version}"

                    // Si la versión que esperabas es diferente a la que obtuviste, tomar una acción
                    // Por ejemplo: si esperabas un incremento, pero se mantuvo igual, puedes notificar al equipo.
                    // O bien, puedes tratarlo como un caso válido que no requiere actualización.
                }
            }
        }


        // ******** Nuevo Stage: Obtener compatibilidad ********
        stage('Obtener compatibilidad') {
            steps {
                echo 'Obteniendo configuración de compatibilidad desde Schema Registry...'
                script {
                    // Se consulta el Schema Registry para extraer el nivel de compatibilidad configurado para el subject
                    def output = sh(
                        script: "curl -s ${SCHEMA_REGISTRY_URL}/config/${SUBJECT_NAME} | jq -r '.compatibilityLevel'",
                        returnStdout: true
                    ).trim()
                    echo "Compatibilidad configurada: ${output}"

                    // Determina a quién notificar como grupo prioritario según el nivel de compatibilidad
                    if (output.startsWith("BACKWARD")) {
                        echo "🔔 Notificando a consumidores (prioritarios) para que actualicen primero..."
                    } else if (output.startsWith("FORWARD")) {
                        echo "🔔 Notificando a productores (prioritarios) para que actualicen primero..."
                    } else if (output.startsWith("FULL")) {
                        echo "🔔 Notificando a ambos grupos para actualización simultánea..."
                    } else {
                        echo "⚠️ Compatibilidad no reconocida. Notificando a todos por precaución."
                    }
                }
            }
        }

        // ******** Nuevo Stage: Verificar actualización de esquemas ********
        stage('Verificar actualización de esquemas') {
            steps {
                echo 'Verificando que el grupo prioritario se haya actualizado...'
                script {
                    // Define la IP del host a la que los servicios están expuestos (debe ser accesible desde Jenkins)
                    def hostIP = "192.168.1.142"  // Ajustar según corresponda
                    // Define los puertos de los servicios según el grupo. Aquí se ejemplifica:
                    def producerPorts = [8090]   // Ejemplo: productores
                    def consumerPorts = [8091]   // Ejemplo: consumidores

                    // Obtener de nuevo la compatibilidad para decidir a quién verificar
                    def compatibility = sh(
                        script: "curl -s ${SCHEMA_REGISTRY_URL}/config/${SUBJECT_NAME} | jq -r '.compatibilityLevel'",
                        returnStdout: true
                    ).trim()

                    echo "Compatibilidad detectada: ${compatibility}"

                    // Seleccionar el grupo prioritario y el grupo secundario en función de la compatibilidad
                    def portsToCheck = []
                    def nextGroup = ""

                    if (compatibility.startsWith("BACKWARD")) {
                        // BACKWARD: Los consumidores deben actualizar primero
                        portsToCheck = consumerPorts
                        nextGroup = "productores"
                    } else if (compatibility.startsWith("FORWARD")) {
                        // FORWARD: Los productores deben actualizar primero
                        portsToCheck = producerPorts
                        nextGroup = "consumidores"
                    } else if (compatibility.startsWith("FULL")) {
                        // FULL: Ambos grupos se actualizan simultáneamente
                        portsToCheck = consumerPorts + producerPorts
                        nextGroup = null
                    } else {
                        echo "Compatibilidad desconocida, se verifican todos los servicios..."
                        portsToCheck = consumerPorts + producerPorts
                        nextGroup = null
                    }

                    def allUpdated = true

                    // Se revisa cada servicio del grupo prioritario (o ambos si FULL)
                    for (port in portsToCheck) {
                        def response = sh(script: "curl -s http://${hostIP}:$port/schema-status", returnStdout: true).trim()
                        echo "${hostIP}:$port → $response"
                        if (!response.contains("Schema is up-to-date")) {
                            echo "❌ El servicio en el puerto $port NO está actualizado"
                            allUpdated = false
                        }
                    }

                    if (!allUpdated) {
                        error("Al menos un servicio del grupo prioritario tiene un esquema desactualizado.")
                    }

                    echo "✅ Todos los servicios del grupo prioritario están actualizados."

                    // Si existe un grupo secundario, notificarlo para proceder con su actualización
                    if (nextGroup != null) {
                        echo "🔔 Notificando al grupo secundario (${nextGroup}) para que proceda con la actualización..."
                        // Aquí se puede agregar una notificación real (por ejemplo, con Slack o email)
                    } else {
                        echo "🔔 No se requiere notificar a un grupo secundario; la actualización es conjunta."
                    }
                }
            }
        }
    }

    post {
        success {
            echo "✅ Proceso completado exitosamente. Los esquemas fueron descargados, comparados, validados y se verificó la actualización del grupo prioritario."
        }
        failure {
            echo "❌ Proceso fallido. Revisar los logs para más detalles."
        }
    }
}
