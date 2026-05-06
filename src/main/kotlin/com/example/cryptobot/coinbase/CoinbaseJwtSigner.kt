package com.example.cryptobot.coinbase

import com.example.cryptobot.config.CoinbaseProperties
import com.nimbusds.jose.JOSEObjectType
import com.nimbusds.jose.JWSAlgorithm
import com.nimbusds.jose.JWSHeader
import com.nimbusds.jose.crypto.ECDSASigner
import com.nimbusds.jwt.JWTClaimsSet
import com.nimbusds.jwt.SignedJWT
import org.bouncycastle.openssl.PEMParser
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter
import org.springframework.stereotype.Component
import java.io.StringReader
import java.security.Security
import java.security.interfaces.ECPrivateKey
import java.time.Instant
import java.util.Date
import java.util.UUID
import org.bouncycastle.jce.provider.BouncyCastleProvider

@Component
class CoinbaseJwtSigner(private val props: CoinbaseProperties) {
    init {
        if (Security.getProvider("BC") == null) Security.addProvider(BouncyCastleProvider())
    }

    fun sign(method: String, requestPath: String): String {
        require(props.apiKeyName.isNotBlank()) { "COINBASE_API_KEY_NAME is required" }
        require(props.privateKeyPem.isNotBlank()) { "COINBASE_PRIVATE_KEY_PEM is required" }

        val now = Instant.now()
        val uri = "${method.uppercase()} api.coinbase.com$requestPath"
        val claims = JWTClaimsSet.Builder()
            .issuer("cdp")
            .subject(props.apiKeyName)
            .notBeforeTime(Date.from(now))
            .expirationTime(Date.from(now.plusSeconds(120)))
            .claim("uri", uri)
            .build()

        val header = JWSHeader.Builder(JWSAlgorithm.ES256)
            .type(JOSEObjectType.JWT)
            .keyID(props.apiKeyName)
            .customParam("nonce", UUID.randomUUID().toString())
            .build()

        val jwt = SignedJWT(header, claims)
        jwt.sign(ECDSASigner(loadPrivateKey()))
        return jwt.serialize()
    }

    private fun loadPrivateKey(): ECPrivateKey {
        val normalizedPem = props.privateKeyPem.replace("\\n", "\n")
        PEMParser(StringReader(normalizedPem)).use { parser ->
            val obj = parser.readObject() ?: error("Could not parse Coinbase private key PEM")
            val converter = JcaPEMKeyConverter().setProvider("BC")
            val privateKey = when (obj) {
                is org.bouncycastle.openssl.PEMKeyPair -> converter.getKeyPair(obj).private
                is org.bouncycastle.asn1.pkcs.PrivateKeyInfo -> converter.getPrivateKey(obj)
                is java.security.PrivateKey -> obj
                else -> error("Unsupported private key object: ${obj::class.qualifiedName}")
            }
            return privateKey as ECPrivateKey
        }
    }
}
