package org.apache.hadoop.hdds.conf;

/**
 * Available config tags.
 * <p>
 * Note: the values are defined in ozone-default.xml by hadoop.tags.custom.
 */
public enum ConfigTag {
  OZONE,
  MANAGEMENT,
  SECURITY,
  PERFORMANCE,
  DEBUG,
  CLIENT,
  SERVER,
  OM,
  SCM,
  CRITICAL,
  RATIS,
  CONTAINER,
  REQUIRED,
  REST,
  STORAGE,
  PIPELINE,
  STANDALONE,
  S3GATEWAY,
  DATANODE,
  RECON
}
