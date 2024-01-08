/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.kinesis.enrichments

import java.util.Base64

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}

case object Javascript extends Enrichment {
  private val base64Encoder = Base64.getEncoder()

  val script = """
    const cipher = javax.crypto.Cipher.getInstance("AES/CBC/PKCS5Padding")
    const generator = javax.crypto.KeyGenerator.getInstance("AES")
    generator.init(new java.security.SecureRandom())
    const key = generator.generateKey()
    cipher.init(javax.crypto.Cipher.ENCRYPT_MODE, key, new javax.crypto.spec.IvParameterSpec(key.getEncoded()))

    function process(event){
      var appId = event.getApp_id()
      if (appId == null) {
        return []
      } else {
        return [ {
          schema: "iglu:com.snowplowanalytics.snowplow/identify/jsonschema/1-0-0",
          data: { id:  new java.lang.String(cipher.doFinal(appId.getBytes())) }
        } ]
      }
    }
  """
  val config = s"""
    {
      "schema": "iglu:com.snowplowanalytics.snowplow/javascript_script_config/jsonschema/1-0-0",
      "data": {
        "vendor": "com.snowplowanalytics.snowplow",
        "name": "javascript_script_config",
        "enabled": true,
        "parameters": {
          "script": "${new String(base64Encoder.encode(script.getBytes()))}"
        }
      }
    }
  """
  val outputSchema = SchemaKey(
    "com.snowplowanalytics.snowplow",
    "identify",
    "jsonschema",
    SchemaVer.Full(1, 0, 0)
  )
}
