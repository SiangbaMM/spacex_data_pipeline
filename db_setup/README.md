# Configuration Schemachange - SpaceX Data Project

## Résolution du problème de versioning

Ce répertoire contient la configuration schemachange mise à jour pour résoudre le problème de versions dupliquées.

### Problème initial

```
ValueError: The script version 1_0 exists more than once (second instance db_setup\db\V1_0\V1_0__02_uat_database_management.sql)
```

### Solution mise en place

#### 1. Configuration centralisée avec `schemachange-config.yml`

- Variables d'environnement dynamiques
- Configuration adaptative selon l'environnement (dev/uat/prd)
- Tailles de warehouses différenciées par environnement

#### 2. Scripts variabilisés dans `db/V1_1/`

- **V1_1\_\_database_management.sql** : Création des rôles, bases de données et warehouses
- **V1_2\_\_custom_users_creation.sql** : Création des utilisateurs personnalisés

#### 3. Workflows GitHub Actions séparés

- **schemachange_dev.yml** : Déploiement automatique sur la branche `dev`
- **schemachange_uat.yml** : Déploiement automatique sur la branche `uat`
- **schemachange_prd.yml** : Déploiement automatique sur les branches `main`/`master`

## Structure des fichiers

```
db_setup/
├── schemachange-config.yml          # Configuration centralisée
├── db/
│   ├── V1_0/                        # Anciens scripts (à supprimer après migration)
│   │   ├── V1_0__01_dev_database_management.sql
│   │   ├── V1_0__02_uat_database_management.sql
│   │   └── ...
│   └── V1_1/                        # Nouveaux scripts variabilisés
│       ├── V1_1__database_management.sql
│       └── V1_2__custom_users_creation.sql
└── README.md
```

## Variables d'environnement

Les scripts utilisent la syntaxe schemachange pour les variables d'environnement :

- `&{ENVIRONMENT}` : Environnement actuel (dev/uat/prd)
- Les noms sont construits dynamiquement : `spacex_data_&{ENVIRONMENT}`
- Les tailles de warehouses sont déterminées par des conditions CASE dans le SQL
- Exemples de résolution :
  - `spacex_data_&{ENVIRONMENT}` → `spacex_data_dev` (si ENVIRONMENT=dev)
  - `spacex_data_&{ENVIRONMENT}_admin` → `spacex_data_dev_admin` (si ENVIRONMENT=dev)

## Utilisation

### Déploiement automatique via GitHub Actions

1. Pousser les modifications sur la branche correspondante :
   - `dev` → Déploiement DEV
   - `uat` → Déploiement UAT
   - `main`/`master` → Déploiement PRD

### Déploiement manuel local

```bash
# Pour l'environnement DEV
export ENVIRONMENT=dev
schemachange deploy \
  -f db_setup \
  --config-file db_setup/schemachange-config.yml \
  -a YOUR_ACCOUNT \
  -u YOUR_USER \
  -p YOUR_PASSWORD \
  -r YOUR_ROLE \
  -w YOUR_WAREHOUSE \
  -d YOUR_DATABASE \
  --verbose

# Pour l'environnement UAT
export ENVIRONMENT=uat
schemachange deploy ...

# Pour l'environnement PRD
export ENVIRONMENT=prd
schemachange deploy ...
```

## Migration depuis l'ancienne structure

1. ✅ Créer la configuration `schemachange-config.yml`
2. ✅ Créer les scripts variabilisés dans `V1_1/`
3. ✅ Mettre à jour les workflows GitHub Actions
4. 🔄 Tester le déploiement sur DEV
5. ⏳ Supprimer l'ancien répertoire `V1_0/` après validation
6. ⏳ Déployer sur UAT puis PRD

## Avantages de cette approche

- ✅ **Résolution du conflit de versions** : Plus de doublons de versions
- ✅ **Maintenance simplifiée** : Un seul jeu de scripts pour tous les environnements
- ✅ **Configuration centralisée** : Toutes les variables dans un seul fichier
- ✅ **Déploiement automatisé** : CI/CD par environnement
- ✅ **Évolutivité** : Facile d'ajouter de nouveaux environnements
