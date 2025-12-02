namespace Helpers
{
    public static class MaskHelper
    {
        public static string MaskEmail(string email)
        {
            if (string.IsNullOrWhiteSpace(email) || !email.Contains("@"))
                return email;

            var parts = email.Split('@');
            var user = parts[0];
            var domain = parts[1];

            if (user.Length <= 2)
                return "***@" + domain;

            return user.Substring(0, 2) + new string('*', user.Length - 2) + "@" + domain;
        }

        public static string MaskPhoneNumber(string phoneNumber)
        {
            if (string.IsNullOrWhiteSpace(phoneNumber) || phoneNumber.Length < 4)
                return "****";

            return new string('*', phoneNumber.Length - 4) + phoneNumber[^4..];
        }
    }
}