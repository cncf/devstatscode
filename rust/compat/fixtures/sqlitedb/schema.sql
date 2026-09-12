-- Grafana `dashboard` and `dashboard_tag` tables as found in a real DevStats
-- Grafana SQLite database (https://devstats.cncf.io/backups/grafana.<project>.db).
CREATE TABLE `dashboard` (
`id` INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL
, `version` INTEGER NOT NULL
, `slug` TEXT NOT NULL
, `title` TEXT NOT NULL
, `data` TEXT NOT NULL
, `org_id` INTEGER NOT NULL
, `created` DATETIME NOT NULL
, `updated` DATETIME NOT NULL
, `updated_by` INTEGER NULL, `created_by` INTEGER NULL, `gnet_id` INTEGER NULL, `plugin_id` TEXT NULL, `folder_id` INTEGER NOT NULL DEFAULT 0, `is_folder` INTEGER NOT NULL DEFAULT 0, `has_acl` INTEGER NOT NULL DEFAULT 0, `uid` TEXT NULL);
CREATE TABLE `dashboard_tag` (
`id` INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL
, `dashboard_id` INTEGER NOT NULL
, `term` TEXT NOT NULL
);
CREATE INDEX `IDX_dashboard_gnet_id` ON `dashboard` (`gnet_id`);
CREATE INDEX `IDX_dashboard_is_folder` ON `dashboard` (`is_folder`);
CREATE INDEX `IDX_dashboard_org_id` ON `dashboard` (`org_id`);
CREATE INDEX `IDX_dashboard_org_id_plugin_id` ON `dashboard` (`org_id`,`plugin_id`);
CREATE INDEX `IDX_dashboard_tag_dashboard_id` ON `dashboard_tag` (`dashboard_id`);
CREATE INDEX `IDX_dashboard_title` ON `dashboard` (`title`);
CREATE UNIQUE INDEX `UQE_dashboard_org_id_folder_id_title` ON `dashboard` (`org_id`,`folder_id`,`title`);
CREATE UNIQUE INDEX `UQE_dashboard_org_id_uid` ON `dashboard` (`org_id`,`uid`);
