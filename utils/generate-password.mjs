import crypto from "crypto";

export const generatePassword = (length = 10) =>
  crypto.randomBytes(length).toString("base64").slice(0, length);
