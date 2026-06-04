import { MapPin } from 'lucide-react';

interface Location {
  id: string;
  name: string;
}

interface LocationSelectorProps {
  locations: Location[];
  selectedLocation: string;
  onLocationChange: (locationId: string) => void;
}

export function LocationSelector({ locations, selectedLocation, onLocationChange }: LocationSelectorProps) {
  return (
    <div className="flex items-center gap-2">
      <MapPin className="w-5 h-5 text-gray-500" />
      <select
        value={selectedLocation}
        onChange={(e) => onLocationChange(e.target.value)}
        className="px-4 py-2 border border-gray-300 rounded-lg bg-white text-gray-900 focus:outline-none focus:ring-2 focus:ring-blue-500"
      >
        {locations.map((location) => (
          <option key={location.id} value={location.id}>
            {location.name}
          </option>
        ))}
      </select>
    </div>
  );
}
