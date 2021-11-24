// Helper definition to workaround VS2019 bug
// CS0518 Predefined type 'System.Runtime.CompilerServices.IsExternalInit' is not defined or imported
// See: https://stackoverflow.com/a/64749403/
namespace System.Runtime.CompilerServices
{
    internal static class IsExternalInit { }
}
