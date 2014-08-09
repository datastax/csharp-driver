using System;
using System.Reflection;
using CqlPoco.IntegrationTests.Pocos;
using CqlPoco.IntegrationTests.TestData;
using FluentAssertions.Equivalency;

namespace CqlPoco.IntegrationTests.Assertions
{
    /// <summary>
    /// Rule for how to match properties between FluentUser and TestUser.
    /// </summary>
    public class FluentUserToTestUserMatchingRule : IMatchingRule
    {
        private static readonly Type TestUserType = typeof (TestUser);
        private static readonly Type FluentUserType = typeof (FluentUser);
        public static readonly PropertyInfo TestUserUserIdProperty = TestUserType.GetProperty("UserId");
        public static readonly PropertyInfo TestUserPreferredContactMethodProperty = TestUserType.GetProperty("PreferredContactMethod");

        public static FluentUserToTestUserMatchingRule Instance { get; private set; }

        static FluentUserToTestUserMatchingRule()
        {
            Instance = new FluentUserToTestUserMatchingRule();
        }

        public PropertyInfo Match(PropertyInfo subjectProperty, object expectation, string propertyPath)
        {
            if (expectation.GetType() != TestUserType)
                return null;

            if (subjectProperty.DeclaringType != FluentUserType)
                return null;

            if (subjectProperty.Name == "Id")
                return TestUserUserIdProperty;

            if (subjectProperty.Name == "PreferredContact")
                return TestUserPreferredContactMethodProperty;

            return null;
        }
    }
}
