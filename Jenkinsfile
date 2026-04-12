// Ez a teljes Jenkins pipeline definíciója.
pipeline {
    // Azt mondja, hogy bármely elérhető Jenkins agenten futhat. Helyi, egyszerű Jenkinsnél ez tipikusan maga a beépített node lesz.
    agent any
    // Itt környezeti változót definiálunk a pipeline számára.
    environment {
        VENV_DIR = ".venv"
    }

    stages {
        // A forráskód lehúzása.
        stage('Checkout') {
            steps {
                // A Jenkins az aktuális pipeline-hoz tartozó repo tartalmát checkoutolja.
                checkout scm
            }
        }
        // Létrehozza a virtual environmentet, frissíti a pip-et, és telepíti a dependency-ket.
        stage('Set up Python environment') {
            steps {
                sh '''
                    python3 -m venv ${VENV_DIR}
                    . ${VENV_DIR}/bin/activate
                    pip install --upgrade pip
                    pip install -r requirements.txt
                '''
            }
        }
        // Futtatja a teszteket:
        stage('Run tests') {
            steps {
                sh '''
                    . ${VENV_DIR}/bin/activate
                    pytest -v
                '''
            }
        }
        // Lefuttatja a fő adatpipeline-t.
        stage('Run pipeline') {
            steps {
                sh '''
                    . ${VENV_DIR}/bin/activate
                    python -m src.pipeline
                '''
            }
        }
        // Lefuttatja a SQL réteget.
        stage('Load to SQLite') {
            steps {
                sh '''
                    . ${VENV_DIR}/bin/activate
                    python -m src.load_to_sqlite
                '''
            }
        }
    }
    /* post { always { ... } }
    A pipeline végén artifactként eltárolja a fontos outputokat. Ez nagyon hasznos, mert Jenkinsből vissza tudjuk nézni a generált fájlokat. */
    post {
        always {
            archiveArtifacts artifacts: 'logs/*.csv, data/processed/*.csv, data/rejected/*.csv, data/sqlite/*.db', fingerprint: true
        }
        success {
            echo 'Pipeline completed successfully.'
        }
        failure {
            echo 'Pipeline failed.'
        }
    }
}
