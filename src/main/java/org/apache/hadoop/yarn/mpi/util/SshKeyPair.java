package org.apache.hadoop.yarn.mpi.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.interfaces.RSAPublicKey;

import org.apache.commons.codec.binary.Base64;

/**
 * The class for generating key pairs used for ssh authentication
 *
 * @author Steven
 *
 */
public class SshKeyPair {
  private final KeyPair keyPair;
  private final String username;
  private final String hostname;

  public SshKeyPair(String username, String hostname)
      throws NoSuchAlgorithmException {
    KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
    keyPairGenerator.initialize(2048, SecureRandom.getInstance("SHA1PRNG"));
    keyPair = keyPairGenerator.generateKeyPair();
    this.username = username;
    this.hostname = hostname;
  }

  public String getPublicKeyFileContent() throws IOException {
    StringBuilder stringBuilder = new StringBuilder("ssh-rsa ");
    stringBuilder
    .append(Base64
        .encodeBase64String(encodePublicKey((RSAPublicKey) keyPair
            .getPublic())).replaceAll("[\\n\\r]", ""));
    if (username != null && hostname != null) {
      stringBuilder.append(' ');
      stringBuilder.append(username + "@" + hostname);
    }
    return stringBuilder.toString();
  }

  public String getPrivateKeyFileContent() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("-----BEGIN RSA PRIVATE KEY-----\n");
    stringBuilder.append(Base64.encodeBase64String(keyPair.getPrivate()
        .getEncoded()).replaceAll("[\\r]", ""));
    stringBuilder.append("-----END RSA PRIVATE KEY-----\n");
    return stringBuilder.toString();
  }

  private static byte[] encodePublicKey(RSAPublicKey key) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    /* encode the "ssh-rsa" string */
    byte[] sshrsa = new byte[] { 0, 0, 0, 7, 's', 's', 'h', '-', 'r', 's', 'a' };
    out.write(sshrsa);
    /* Encode the public exponent */
    BigInteger e = key.getPublicExponent();
    byte[] data = e.toByteArray();
    encodeUInt32(data.length, out);
    out.write(data);
    /* Encode the modulus */
    BigInteger m = key.getModulus();
    data = m.toByteArray();
    encodeUInt32(data.length, out);
    out.write(data);
    return out.toByteArray();
  }

  private static void encodeUInt32(int value, OutputStream out)
      throws IOException {
    byte[] tmp = new byte[4];
    tmp[0] = (byte) ((value >>> 24) & 0xff);
    tmp[1] = (byte) ((value >>> 16) & 0xff);
    tmp[2] = (byte) ((value >>> 8) & 0xff);
    tmp[3] = (byte) (value & 0xff);
    out.write(tmp);
  }
}
