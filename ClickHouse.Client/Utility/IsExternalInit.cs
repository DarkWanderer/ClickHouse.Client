// Helper definition to workaround VS2019 bug
// CS0518 Predefined type 'System.Runtime.CompilerServices.IsExternalInit' is not defined or imported
// See: https://stackoverflow.com/a/64749403/
#pragma warning disable IDE0130 // Namespace does not match folder structure
namespace System.Runtime.CompilerServices;
#pragma warning restore IDE0130 // Namespace does not match folder structure

internal static class IsExternalInit { }
