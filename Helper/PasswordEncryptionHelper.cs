using System;
using System.Security.Cryptography;
using System.Threading.Tasks;

namespace DataMigration.Helper
{
    public static class PasswordEncryptionHelper
    {
        private const int SaltSize = 16;
        private const int KeySize = 32;
        private const int Iterations = 15000; // Increased from 10000 for better security

        public static (string Hash, string Salt) EncryptPassword(string password)
        {
            return EncryptPassword(password, Iterations);
        }

        // Overload that allows specifying iteration count (for fast migration mode)
        public static (string Hash, string Salt) EncryptPassword(string password, int iterations)
        {
            if (string.IsNullOrEmpty(password))
                throw new ArgumentNullException(nameof(password), "Password cannot be null or empty");

            // Generate a random salt
            using var rng = RandomNumberGenerator.Create();
            byte[] saltBytes = new byte[SaltSize];
            rng.GetBytes(saltBytes);

            // Hash the password with the salt
            using var deriveBytes = new Rfc2898DeriveBytes(password, saltBytes, iterations, HashAlgorithmName.SHA256);
            byte[] hashBytes = deriveBytes.GetBytes(KeySize);

            // Return base64-encoded versions
            return (Convert.ToBase64String(hashBytes), Convert.ToBase64String(saltBytes));
        }

        public static void CreatePasswordHash(string password, out string hash, out string salt)
        {
            // Ensure password is not null
            if (string.IsNullOrEmpty(password))
            {
                hash = string.Empty;
                salt = string.Empty;
                return;
            }

            // This method provides backward compatibility with the out parameter pattern
            var (passwordHash, passwordSalt) = EncryptPassword(password);
            hash = passwordHash;
            salt = passwordSalt;
        }

        public static async Task<bool> VerifyPassword(string password, string storedHash, string storedSalt)
        {
            if (string.IsNullOrEmpty(password) || string.IsNullOrEmpty(storedHash) || string.IsNullOrEmpty(storedSalt))
                return false;

            try
            {
                return await Task.Run(() =>
                {
                    byte[] saltBytes = Convert.FromBase64String(storedSalt);
                    byte[] storedHashBytes = Convert.FromBase64String(storedHash);

                    // Re-generate hash with the same salt
                    using var deriveBytes = new Rfc2898DeriveBytes(password, saltBytes, Iterations, HashAlgorithmName.SHA256);
                    byte[] testHashBytes = deriveBytes.GetBytes(KeySize);

                    // Compare byte-by-byte in constant time to prevent timing attacks
                    return CryptographicOperations.FixedTimeEquals(testHashBytes, storedHashBytes);
                });
            }
            catch (Exception)
            {
                // If there's any error (like invalid base64 strings), return false
                return false;
            }
        }

        /// <summary>
        /// Determines if a password needs to be rehashed (for example, if hashing parameters have changed)
        /// </summary>
        /// <param name="storedHash">The stored hash from the database</param>
        /// <param name="storedSalt">The stored salt from the database</param>
        /// <returns>True if the password should be rehashed, false otherwise</returns>
        public static bool NeedsRehash(string storedHash, string storedSalt)
        {
            // In a real implementation, you might check for iteration count or algorithm changes
            // This simplified version just checks for valid hash/salt
            if (string.IsNullOrEmpty(storedHash) || string.IsNullOrEmpty(storedSalt))
                return true;

            try
            {
                // Check if the salt and hash are valid base64 strings of the expected length
                byte[] saltBytes = Convert.FromBase64String(storedSalt);
                byte[] hashBytes = Convert.FromBase64String(storedHash);

                return saltBytes.Length != SaltSize || hashBytes.Length != KeySize;
            }
            catch
            {
                // If there's any error (like invalid base64 strings), rehash is needed
                return true;
            }
        }

        public static async Task<bool> VerifyPasswordHash(string newPassword, string passwordHash, string passwordSalt)
        {
            if (string.IsNullOrEmpty(newPassword) || string.IsNullOrEmpty(passwordHash) || string.IsNullOrEmpty(passwordSalt))
                return false;

            try
            {
                return await VerifyPassword(newPassword, passwordHash, passwordSalt);
            }
            catch (Exception)
            {
                // If there's any error, return false indicating verification failed
                return false;
            }
        }
    }
}
