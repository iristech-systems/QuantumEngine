from quantumorm.fields import IPAddressField

# Test with version parameter
field1 = IPAddressField(version='ipv4')
print(f"Field1 (version='ipv4'): ipv4_only={field1.ipv4_only}, ipv6_only={field1.ipv6_only}")

field2 = IPAddressField(version='ipv6')
print(f"Field2 (version='ipv6'): ipv4_only={field2.ipv4_only}, ipv6_only={field2.ipv6_only}")

field3 = IPAddressField(version='both')
print(f"Field3 (version='both'): ipv4_only={field3.ipv4_only}, ipv6_only={field3.ipv6_only}")

# Test with ipv4_only/ipv6_only parameters
field4 = IPAddressField(ipv4_only=True)
print(f"Field4 (ipv4_only=True): ipv4_only={field4.ipv4_only}, ipv6_only={field4.ipv6_only}")

field5 = IPAddressField(ipv6_only=True)
print(f"Field5 (ipv6_only=True): ipv4_only={field5.ipv4_only}, ipv6_only={field5.ipv6_only}")

field6 = IPAddressField()
print(f"Field6 (default): ipv4_only={field6.ipv4_only}, ipv6_only={field6.ipv6_only}")

print("All tests passed!")